import os
from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager
from pyiceberg.catalog import Catalog, load_catalog

NAMESPACE = os.environ["OSM_ICEBERG_NAMESPACE"]


def _catalog_properties() -> dict[str, str]:
    return {
        "type": "rest",
        "uri": os.environ["OSM_ICEBERG_REST_URI"],
        "warehouse": os.environ["OSM_ICEBERG_WAREHOUSE"],
        "s3.endpoint": os.environ["OSM_S3_ENDPOINT"],
        "s3.access-key-id": os.environ["OSM_S3_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["OSM_S3_SECRET_ACCESS_KEY"],
    }


class IcebergMaintenanceResource(dg.ConfigurableResource):
    """Snapshot-retention housekeeping, same approach as the other code
    locations' resource (pyiceberg's older_than() has no "keep at least N"
    concept, so the expire set is computed manually)."""

    catalog_properties: dict[str, str]
    namespace: str

    def _catalog(self) -> Catalog:
        return load_catalog("rest_catalog", **self.catalog_properties)

    def expire_snapshots(
        self,
        table_name: str,
        max_snapshot_age_weeks: int,
        min_snapshots_to_keep: int,
        context: dg.OpExecutionContext,
    ) -> None:
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


@dg.definitions
def resources() -> dg.Definitions:
    catalog_properties = _catalog_properties()
    return dg.Definitions(
        resources={
            "io_manager": PyArrowIcebergIOManager(
                name="rest_catalog",
                config=IcebergCatalogConfig(properties=catalog_properties),
                namespace=NAMESPACE,
            ),
            "iceberg_maintenance": IcebergMaintenanceResource(
                catalog_properties=catalog_properties, namespace=NAMESPACE
            ),
        }
    )
