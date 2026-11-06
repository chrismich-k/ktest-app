import time
from datetime import UTC, date, datetime, timedelta

import dagster as dg
import pyarrow as pa
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from ktest.entsog.client import EntsogClient
from ktest.entsog.datasets import OPERATIONAL_SCHEMA, parse_operational_day
from ktest.entsog.maintenance import maintenance_defs

TIMEZONE = "Europe/Brussels"
# run tags for a run over a range of partitions (what single-run backfills
# use); RunRequest has no parameter for it
PARTITION_RANGE_START_TAG = "dagster/asset_partition_range_start"
PARTITION_RANGE_END_TAG = "dagster/asset_partition_range_end"
# a day always has values for most points; a day without any means a
# broken response and must not replace stored data
BROKEN_DAY_ATTEMPTS = 3
BROKEN_DAY_RETRY_SECONDS = 30


def _fetch_day(
    context: dg.AssetExecutionContext,
    client: EntsogClient,
    indicator: str,
    gas_day: date,
) -> list[dict]:
    """The records of a gas day as rows."""
    for attempt in range(1, BROKEN_DAY_ATTEMPTS + 1):
        records = client.records(
            "operationaldatas",
            {
                "from": gas_day.isoformat(),
                "to": gas_day.isoformat(),
                "indicator": indicator,
                "periodType": "day",
            },
        )
        rows = parse_operational_day(records, gas_day)
        if any(row["value"] is not None for row in rows):
            return rows
        context.log.warning(
            f"{gas_day}: no values in the response ({len(rows)} records),"
            f" attempt {attempt}"
        )
        if attempt < BROKEN_DAY_ATTEMPTS:
            time.sleep(BROKEN_DAY_RETRY_SECONDS)
    raise ValueError(f"no values for {gas_day}")


class EntsogOperationalDataComponent(dg.Component, dg.Model, dg.Resolvable):
    """One indicator of the ENTSOG operational data (daily values of all
    points and operators), imported into one Iceberg table named after the
    last asset key segment.

    Dagster partitions are gas days, the last one is yesterday. A run over
    a range of days makes one call per day (larger queries are cut off
    silently, see ktest.entsog.client) and replaces those days in a single
    commit. The table is partitioned by year(gas_day). The refresh schedule
    re-fetches the last `refresh_lookback_days` partitions, which picks up
    late values and the change from provisional to confirmed.
    """

    # as named by the API, e.g. "Physical Flow"
    indicator: str
    # the API only has the last five years
    start_date: str
    refresh_cron: str
    refresh_lookback_days: int
    spec: dg.ResolvedAssetSpec

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        indicator = self.indicator
        table_name = self.spec.key.path[-1]
        name = "_".join(self.spec.key.path)

        partitions_def = dg.DailyPartitionsDefinition(
            start_date=self.start_date, timezone=TIMEZONE
        )

        @dg.asset(
            key=self.spec.key,
            description=self.spec.description,
            group_name=self.spec.group_name,
            kinds=self.spec.kinds,
            metadata=self.spec.metadata,
            partitions_def=partitions_def,
            # one call of 1-2 seconds per day
            backfill_policy=dg.BackfillPolicy.multi_run(
                max_partitions_per_run=92
            ),
            # serializes the runs of all ENTSOG assets (limit 1, set
            # separately): spares the API, and Iceberg commits of
            # overlapping runs would conflict
            pool="entsog_api",
            required_resource_keys={"entsog_api", "entsog_iceberg"},
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            key_range = context.partition_key_range
            first_day = date.fromisoformat(key_range.start)
            last_day = date.fromisoformat(key_range.end)
            fetched_at = datetime.now(UTC)

            # converted per day: a quarter of rows as dicts would take a lot
            # of memory
            tables: list[pa.Table] = []
            with context.resources.entsog_api.client() as client:
                gas_day = first_day
                while gas_day <= last_day:
                    rows = _fetch_day(context, client, indicator, gas_day)
                    for row in rows:
                        row["fetched_at"] = fetched_at
                    tables.append(
                        pa.Table.from_pylist(rows, schema=OPERATIONAL_SCHEMA)
                    )
                    gas_day += timedelta(days=1)

            table = pa.concat_tables(tables)
            context.resources.entsog_iceberg.overwrite(
                table_name,
                table,
                overwrite_filter=And(
                    GreaterThanOrEqual(
                        term="gas_day", value=first_day.isoformat()
                    ),
                    LessThan(term="gas_day", value=gas_day.isoformat()),
                ),
                snapshot_properties={
                    "dagster-partition-range": (
                        f"{key_range.start}..{key_range.end}"
                    ),
                },
                year_column="gas_day",
            )
            return dg.MaterializeResult(
                metadata={
                    "row_count": table.num_rows,
                    "value_count": table.num_rows
                    - table.column("value").null_count,
                    "day_count": len(tables),
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

        maintenance_job, maintenance_schedule = maintenance_defs(
            name, table_name, TIMEZONE
        )
        return dg.Definitions(
            assets=[_asset],
            jobs=[refresh_job, maintenance_job],
            schedules=[_refresh_schedule, maintenance_schedule],
        )
