import time
from datetime import UTC, date, datetime, timedelta
from typing import Literal

import dagster as dg
import pyarrow as pa
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    GreaterThanOrEqual,
    In,
    LessThan,
)

from ktest.gie.client import GieClient, IncompleteResponseError
from ktest.gie.datasets import DATASETS, Dataset, ParsedDay, parse_day

# gas days run 06:00 to 06:00 CET/CEST; the platforms publish a gas day in
# the evening after it ended
TIMEZONE = "Europe/Brussels"
# run tags for a run over a range of partitions (what single-run backfills
# use); RunRequest has no parameter for it
PARTITION_RANGE_START_TAG = "dagster/asset_partition_range_start"
PARTITION_RANGE_END_TAG = "dagster/asset_partition_range_end"
# the API occasionally returns a day's tree without facilities or a response
# without its top-level fields (seen in runs over a month, the same day was
# fine right after); retry such days
BROKEN_DAY_ATTEMPTS = 3
BROKEN_DAY_RETRY_SECONDS = 30


def _fetch_day(
    context: dg.AssetExecutionContext,
    client: GieClient,
    dataset: Dataset,
    gas_day: date,
    countries: list[str] | None,
) -> ParsedDay | None:
    """The facility rows of a gas day, None if it is not published yet."""
    for attempt in range(1, BROKEN_DAY_ATTEMPTS + 1):
        try:
            result = client.query(dataset.host, {"date": gas_day.isoformat()})
        except IncompleteResponseError as e:
            problem = str(e)
        else:
            if gas_day > result.latest_gas_day:
                return None
            parsed = parse_day(dataset, result.data, gas_day, countries)
            # the tree always lists the facilities, with "-" if they have
            # no data; an empty day means a broken response and must not
            # replace stored data
            if parsed.rows:
                return parsed
            nodes = [
                (node.get("code"), len(node.get("children") or []))
                for node in result.data
            ]
            problem = (
                "no facilities in the response (top-level nodes and their"
                f" child counts: {nodes})"
            )
        context.log.warning(f"{gas_day}: {problem}, attempt {attempt}")
        if attempt < BROKEN_DAY_ATTEMPTS:
            time.sleep(BROKEN_DAY_RETRY_SECONDS)
    raise ValueError(f"no usable response for {gas_day}: {problem}")


class GieComponent(dg.Component, dg.Model, dg.Resolvable):
    """One GIE transparency platform dataset at facility level, imported
    into one Iceberg table named after the last asset key segment.

    Dagster partitions are gas days. The API returns all facilities of a
    gas day in one call, so a run over a range of days makes one call per
    day and replaces those days in a single commit; with `countries` set,
    only the facilities of these countries are kept and replaced. The
    table is partitioned by year(gas_day), see ktest.gie.iceberg. The
    refresh schedule re-fetches the last `refresh_lookback_days`
    partitions, which picks up late publications and corrections.
    """

    dataset: Literal["agsi", "alsi"]
    start_date: str
    # partitions beyond the last complete day (yesterday); 0 for gas
    # storage, which is published after the gas day
    end_offset: int
    refresh_cron: str
    refresh_lookback_days: int
    # country codes as used by the API (e.g. DE, GB*); all if omitted
    countries: list[str] | None = None
    spec: dg.ResolvedAssetSpec

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        dataset = DATASETS[self.dataset]
        table_name = self.spec.key.path[-1]
        name = "_".join(self.spec.key.path)
        countries = self.countries

        partitions_def = dg.DailyPartitionsDefinition(
            start_date=self.start_date,
            timezone=TIMEZONE,
            end_offset=self.end_offset,
        )

        @dg.asset(
            key=self.spec.key,
            description=self.spec.description,
            group_name=self.spec.group_name,
            kinds=self.spec.kinds,
            metadata=self.spec.metadata,
            partitions_def=partitions_def,
            # one API call per day, about one minute per 50 days
            backfill_policy=dg.BackfillPolicy.multi_run(
                max_partitions_per_run=366
            ),
            # serializes the runs of all GIE assets (limit 1, set
            # separately): the API rate limit is shared by both platforms,
            # and Iceberg commits of overlapping runs would conflict
            pool="gie_api",
            required_resource_keys={"gie_api", "gie_iceberg"},
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            key_range = context.partition_key_range
            first_day = date.fromisoformat(key_range.start)
            last_day = date.fromisoformat(key_range.end)
            fetched_at = datetime.now(UTC)

            rows: list[dict] = []
            skipped_without_code = 0
            with context.resources.gie_api.client() as client:
                gas_day = first_day
                while gas_day <= last_day:
                    parsed = _fetch_day(
                        context, client, dataset, gas_day, countries
                    )
                    if parsed is None:
                        break
                    for row in parsed.rows:
                        row["fetched_at"] = fetched_at
                    rows.extend(parsed.rows)
                    skipped_without_code += parsed.skipped_without_code
                    gas_day += timedelta(days=1)

            # a gas day is published in the evening after it ended; days of
            # the range not published yet are left as they are (the next
            # refresh picks them up)
            end_day = gas_day
            if end_day == first_day:
                raise dg.Failure(f"gas day {first_day} is not published yet")
            if end_day <= last_day:
                context.log.info(
                    f"not published yet: {end_day}..{last_day}, skipped"
                )
            overwrite_filter: BooleanExpression = And(
                GreaterThanOrEqual(
                    term="gas_day", value=first_day.isoformat()
                ),
                LessThan(term="gas_day", value=end_day.isoformat()),
            )
            if countries is not None:
                overwrite_filter = And(
                    overwrite_filter,
                    In(term="country_code", literals=countries),
                )
            table = pa.Table.from_pylist(rows, schema=dataset.schema)
            context.resources.gie_iceberg.overwrite(
                table_name,
                table,
                overwrite_filter=overwrite_filter,
                snapshot_properties={
                    "dagster-partition-range": (
                        f"{key_range.start}..{key_range.end}"
                    ),
                },
            )
            return dg.MaterializeResult(
                metadata={
                    "row_count": table.num_rows,
                    "day_count": (end_day - first_day).days,
                    "unpublished_day_count": (last_day - end_day).days + 1,
                    "skipped_without_code": skipped_without_code,
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
            execution_timezone=TIMEZONE,
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
            required_resource_keys={"gie_iceberg"},
        )
        def _expire_snapshots_op(context: dg.OpExecutionContext) -> None:
            context.resources.gie_iceberg.expire_snapshots(
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
            execution_timezone=TIMEZONE,
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )

        return dg.Definitions(
            assets=[_asset],
            jobs=[refresh_job, _maintenance_job],
            schedules=[_refresh_schedule, maintenance_schedule],
        )
