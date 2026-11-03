from datetime import UTC, date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

import dagster as dg
import pyarrow as pa
from pyiceberg.expressions import (
    And,
    GreaterThanOrEqual,
    In,
    LessThan,
)

from ktest.entsoe.client import format_period, request_chunks
from ktest.entsoe.datasets import DATASETS

# market days are CET/CEST days
MARKET_TIMEZONE = "Europe/Berlin"
# run tags for a run over a range of partitions (what single-run backfills
# use); RunRequest has no parameter for it
PARTITION_RANGE_START_TAG = "dagster/asset_partition_range_start"
PARTITION_RANGE_END_TAG = "dagster/asset_partition_range_end"


def _market_day_start(day: str) -> datetime:
    return datetime.combine(
        date.fromisoformat(day), time(), tzinfo=ZoneInfo(MARKET_TIMEZONE)
    ).astimezone(UTC)


class Zone(dg.Model):
    name: str
    eic: str
    # first and last market day the zone exists (e.g. DE-AT-LU until
    # 2018-09-30); open-ended if omitted
    valid_from: str | None = None
    valid_to: str | None = None

    def clip(
        self, start: datetime, end: datetime
    ) -> tuple[datetime, datetime]:
        if self.valid_from is not None:
            start = max(start, _market_day_start(self.valid_from))
        if self.valid_to is not None:
            last_day = date.fromisoformat(self.valid_to) + timedelta(days=1)
            end = min(end, _market_day_start(last_day.isoformat()))
        return start, end


class EntsoeComponent(dg.Component, dg.Model, dg.Resolvable):
    """One ENTSO-E Transparency Platform data item for a list of zones,
    imported into one Iceberg table named after the last asset key
    segment.

    Dagster partitions are market days; a run over a range of days (backfill
    or the refresh schedule) fetches every zone for the whole range and
    replaces those days of the configured zones in a single commit. The
    table is partitioned by month(ts), see ktest.entsoe.iceberg. The refresh
    schedule re-fetches the last `refresh_lookback_days` partitions, which
    picks up late publications and corrections.
    """

    document_type: Literal["A44"]
    start_date: str
    # partitions beyond the last complete day (yesterday), e.g. 2 for
    # day-ahead data published for tomorrow
    end_offset: int
    refresh_cron: str
    refresh_lookback_days: int
    zones: list[Zone]
    spec: dg.ResolvedAssetSpec

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        dataset = DATASETS[self.document_type]
        table_name = self.spec.key.path[-1]
        name = "_".join(self.spec.key.path)
        zones = self.zones
        all_zone_eics = [zone.eic for zone in zones]

        partitions_def = dg.DailyPartitionsDefinition(
            start_date=self.start_date,
            timezone=MARKET_TIMEZONE,
            end_offset=self.end_offset,
        )

        @dg.asset(
            key=self.spec.key,
            description=self.spec.description,
            group_name=self.spec.group_name,
            kinds=self.spec.kinds,
            metadata=self.spec.metadata,
            partitions_def=partitions_def,
            # a range of days per run, one API request per zone and 90 days
            backfill_policy=dg.BackfillPolicy.multi_run(
                max_partitions_per_run=92
            ),
            # Iceberg commits use optimistic concurrency per table; runs of
            # this asset overlapping in time would conflict. Serialize
            # writes via a pool with a limit of 1 (set separately).
            pool="entsoe_iceberg_writes",
            required_resource_keys={"entsoe_api", "entsoe_iceberg"},
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            window = context.partition_time_window
            start = window.start.astimezone(UTC)
            end = window.end.astimezone(UTC)
            fetched_at = datetime.now(UTC)

            rows: list[dict] = []
            zones_without_data: list[str] = []
            with context.resources.entsoe_api.client() as client:
                for zone in zones:
                    zone_start, zone_end = zone.clip(start, end)
                    if zone_start >= zone_end:
                        continue
                    zone_rows = 0
                    for chunk_start, chunk_end in request_chunks(
                        zone_start, zone_end
                    ):
                        documents = client.query(
                            {
                                **dataset.query_params(zone.eic),
                                "periodStart": format_period(chunk_start),
                                "periodEnd": format_period(chunk_end),
                            }
                        )
                        for document in documents:
                            for row in dataset.parse(document):
                                if zone_start <= row["ts"] < zone_end:
                                    row["fetched_at"] = fetched_at
                                    rows.append(row)
                                    zone_rows += 1
                    context.log.info(f"{zone.name}: {zone_rows} rows")
                    if zone_rows == 0:
                        zones_without_data.append(zone.name)

            table = pa.Table.from_pylist(rows, schema=dataset.schema)
            context.resources.entsoe_iceberg.overwrite(
                table_name,
                table,
                overwrite_filter=And(
                    GreaterThanOrEqual(term="ts", value=start.isoformat()),
                    LessThan(term="ts", value=end.isoformat()),
                    In(term=dataset.zone_column, literals=all_zone_eics),
                ),
                snapshot_properties={
                    "dagster-partition-range": (
                        f"{context.partition_key_range.start}.."
                        f"{context.partition_key_range.end}"
                    ),
                },
            )
            return dg.MaterializeResult(
                metadata={
                    "row_count": table.num_rows,
                    "period": f"{start.isoformat()}..{end.isoformat()}",
                    "zones_without_data": ", ".join(zones_without_data) or "-",
                }
            )

        refresh_job = dg.define_asset_job(
            name=f"{name}_refresh_job",
            selection=[self.spec.key],
            partitions_def=partitions_def,
        )
        lookback = self.refresh_lookback_days

        @dg.schedule(
            name=f"{name}_refresh_schedule",
            cron_schedule=self.refresh_cron,
            job=refresh_job,
            execution_timezone=MARKET_TIMEZONE,
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )
        def _refresh_schedule(
            context: dg.ScheduleEvaluationContext,
        ) -> dg.RunRequest:
            keys = partitions_def.get_partition_keys(
                current_time=context.scheduled_execution_time
            )[-lookback:]
            return dg.RunRequest(
                tags={
                    PARTITION_RANGE_START_TAG: keys[0],
                    PARTITION_RANGE_END_TAG: keys[-1],
                },
            )

        @dg.op(
            name=f"{name}_expire_snapshots_op",
            required_resource_keys={"entsoe_iceberg"},
        )
        def _expire_snapshots_op(context: dg.OpExecutionContext) -> None:
            context.resources.entsoe_iceberg.expire_snapshots(
                table_name=table_name,
                max_snapshot_age_weeks=4,
                min_snapshots_to_keep=5,
                context=context,
            )

        @dg.job(name=f"{name}_maintenance_job")
        def _maintenance_job():
            _expire_snapshots_op()

        maintenance_schedule = dg.ScheduleDefinition(
            name=f"{name}_maintenance_schedule",
            job=_maintenance_job,
            cron_schedule="0 3 * * 0",
            execution_timezone=MARKET_TIMEZONE,
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )

        return dg.Definitions(
            assets=[_asset],
            jobs=[refresh_job, _maintenance_job],
            schedules=[_refresh_schedule, maintenance_schedule],
        )
