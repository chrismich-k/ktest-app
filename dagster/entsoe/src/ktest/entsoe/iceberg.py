"""Direct pyiceberg writes instead of dagster-iceberg's IO manager.

dagster-iceberg derives the table's partition spec from the length of the
Dagster partition window (daily partitions -> day(ts)), which would mean one
Iceberg partition with a handful of rows per day here. Writing directly
keeps daily Dagster partitions on a table partitioned by month(ts).
"""

import time
from datetime import UTC, datetime, timedelta

import dagster as dg
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import BooleanExpression
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform

COMMIT_ATTEMPTS = 4


class IcebergTablesResource(dg.ConfigurableResource):
    catalog_properties: dict[str, str]
    namespace: str

    def _catalog(self) -> Catalog:
        return load_catalog("rest_catalog", **self.catalog_properties)

    def _table(
        self, catalog: Catalog, table_name: str, schema: pa.Schema
    ) -> Table:
        """The table, created partitioned by month(ts) if missing; columns
        added to the schema are added to the table."""
        identifier = f"{self.namespace}.{table_name}"
        if catalog.table_exists(identifier):
            table = catalog.load_table(identifier)
            with table.update_schema() as update:
                update.union_by_name(schema)
            return table
        with catalog.create_table_transaction(identifier, schema) as txn:
            with txn.update_spec() as update:
                update.add_field("ts", MonthTransform(), "ts_month")
        return catalog.load_table(identifier)

    def overwrite(
        self,
        table_name: str,
        data: pa.Table,
        overwrite_filter: BooleanExpression,
        snapshot_properties: dict[str, str],
    ) -> None:
        """Replaces the rows matching the filter by `data` in one commit."""
        table = self._table(self._catalog(), table_name, data.schema)
        for attempt in range(1, COMMIT_ATTEMPTS + 1):
            try:
                table.overwrite(
                    data,
                    overwrite_filter=overwrite_filter,
                    snapshot_properties=snapshot_properties,
                )
                return
            except CommitFailedException:
                if attempt == COMMIT_ATTEMPTS:
                    raise
                time.sleep(2**attempt)
                table.refresh()

    def expire_snapshots(
        self,
        table_name: str,
        max_snapshot_age_weeks: int,
        min_snapshots_to_keep: int,
        context: dg.OpExecutionContext,
    ) -> None:
        """Snapshot retention, same approach as the other code locations
        (pyiceberg's older_than() has no "keep at least N" on its own)."""
        table = self._catalog().load_table(f"{self.namespace}.{table_name}")
        snapshots = sorted(
            table.metadata.snapshots,
            key=lambda s: s.timestamp_ms,
            reverse=True,
        )
        cutoff_ms = (
            datetime.now(UTC) - timedelta(weeks=max_snapshot_age_weeks)
        ).timestamp() * 1000
        to_expire = [
            s.snapshot_id
            for s in snapshots[min_snapshots_to_keep:]
            if s.timestamp_ms < cutoff_ms
        ]
        if not to_expire:
            context.log.info(f"{table_name}: nothing to expire")
            return
        table.maintenance.expire_snapshots().by_ids(to_expire).commit()
        context.log.info(f"{table_name}: expired {len(to_expire)} snapshot(s)")
