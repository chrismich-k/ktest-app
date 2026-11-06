from datetime import UTC, date, datetime
from typing import Literal
from zoneinfo import ZoneInfo

import dagster as dg
import pyarrow as pa
from pyiceberg.expressions import EqualTo

from ktest.entsog.datasets import SNAPSHOTS, parse_snapshot
from ktest.entsog.maintenance import maintenance_defs

TIMEZONE = "Europe/Brussels"
ZONE = ZoneInfo(TIMEZONE)
PARTITION_TAG = "dagster/partition"


class EntsogSnapshotComponent(dg.Component, dg.Model, dg.Resolvable):
    """Master data of one ENTSOG endpoint as daily snapshots, imported into
    one Iceberg table named after the last asset key segment.

    Dagster partitions are dynamic: one per snapshot date actually
    fetched, the key stored as column `snapshot_date`; the table is
    partitioned by year(snapshot_date). The API only returns the current
    state, so only today's partition can be materialized; a run replaces
    the snapshot of the day.

    A sensor adds today's partition from `fetch_hour` on and requests its
    run in the same evaluation. If the run fails, a run failure sensor
    removes the partition again, and the next evaluation retries.
    """

    endpoint: Literal["interconnections"]
    # local hour (Europe/Brussels) from which today's snapshot is fetched
    fetch_hour: int
    spec: dg.ResolvedAssetSpec

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        endpoint = self.endpoint
        snapshot = SNAPSHOTS[endpoint]
        table_name = self.spec.key.path[-1]
        name = "_".join(self.spec.key.path)

        partitions_name = f"{name}_snapshots"
        partitions_def = dg.DynamicPartitionsDefinition(name=partitions_name)

        @dg.asset(
            key=self.spec.key,
            description=self.spec.description,
            group_name=self.spec.group_name,
            kinds=self.spec.kinds,
            metadata=self.spec.metadata,
            partitions_def=partitions_def,
            pool="entsog_api",
            required_resource_keys={"entsog_api", "entsog_iceberg"},
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            snapshot_date = date.fromisoformat(context.partition_key)
            fetched_at = datetime.now(UTC)
            today = fetched_at.astimezone(ZONE).date()
            if snapshot_date != today:
                raise dg.Failure(
                    f"the API only returns the current state: snapshot"
                    f" {snapshot_date} can only be fetched on that day"
                    f" (today is {today})"
                )

            with context.resources.entsog_api.client() as client:
                records = client.records(endpoint, {})
            if not records:
                raise ValueError(f"no records from {endpoint}")
            rows = parse_snapshot(snapshot, records, snapshot_date)
            for row in rows:
                row["fetched_at"] = fetched_at
            table = pa.Table.from_pylist(rows, schema=snapshot.schema)

            context.resources.entsog_iceberg.overwrite(
                table_name,
                table,
                overwrite_filter=EqualTo(
                    term="snapshot_date", value=snapshot_date.isoformat()
                ),
                snapshot_properties={
                    "dagster-partition": context.partition_key
                },
                year_column="snapshot_date",
            )
            return dg.MaterializeResult(metadata={"row_count": table.num_rows})

        job = dg.define_asset_job(
            name=f"{name}_job",
            selection=[self.spec.key],
            partitions_def=partitions_def,
        )

        fetch_hour = self.fetch_hour
        asset_key = self.spec.key

        # a schedule cannot add dynamic partitions, a sensor can
        @dg.sensor(
            name=f"{name}_sensor",
            job=job,
            minimum_interval_seconds=15 * 60,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _sensor(
            context: dg.SensorEvaluationContext,
        ) -> dg.SensorResult | dg.SkipReason:
            now = datetime.now(ZONE)
            if now.hour < fetch_hour:
                return dg.SkipReason(f"waiting until {fetch_hour}:00")
            key = now.date().isoformat()
            # the partition exists from the run request on; a failed run
            # removes it again (see below)
            if context.instance.has_dynamic_partition(partitions_name, key):
                return dg.SkipReason(f"snapshot {key} exists")
            return dg.SensorResult(
                run_requests=[dg.RunRequest(partition_key=key)],
                dynamic_partitions_requests=[
                    partitions_def.build_add_request([key])
                ],
            )

        # also covers runs launched by hand, e.g. for a partition added by
        # hand for another day
        @dg.run_failure_sensor(
            name=f"{name}_failed_snapshot_sensor",
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _failed_snapshot_sensor(
            context: dg.RunFailureSensorContext,
        ) -> None:
            run = context.dagster_run
            key = run.tags.get(PARTITION_TAG)
            ours = run.job_name == job.name or asset_key in (
                run.asset_selection or ()
            )
            if key is None or not ours:
                return
            if key in context.instance.get_materialized_partitions(asset_key):
                return
            context.instance.delete_dynamic_partition(partitions_name, key)
            context.log.info(f"removed partition {key} of failed run")

        maintenance_job, maintenance_schedule = maintenance_defs(
            name, table_name, TIMEZONE
        )
        return dg.Definitions(
            assets=[_asset],
            jobs=[job, maintenance_job],
            schedules=[maintenance_schedule],
            sensors=[_sensor, _failed_snapshot_sensor],
        )
