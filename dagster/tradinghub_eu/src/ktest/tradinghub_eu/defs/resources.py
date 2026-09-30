import os
from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager
from pyiceberg.catalog import Catalog, load_catalog

# Must be imported (and thus applied) before any Iceberg write happens -
# see the module docstring in iceberg_io_manager.py for why.
from ktest.tradinghub_eu import (
    iceberg_io_manager as _iceberg_io_manager_patch,  # noqa: F401
)

NAMESPACE = "tradinghub_eu"


def _catalog_properties() -> dict[str, str]:
    return {
        "type": "rest",
        "uri": os.environ.get(
            "TRADINGHUB_EU_ICEBERG_REST_URI",
            "http://localhost:9001/iceberg",
        ),
        "warehouse": os.environ["TRADINGHUB_EU_ICEBERG_WAREHOUSE"],
        "s3.endpoint": os.environ["TRADINGHUB_EU_S3_ENDPOINT"],
        "s3.access-key-id": os.environ["TRADINGHUB_EU_S3_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ[
            "TRADINGHUB_EU_S3_SECRET_ACCESS_KEY"
        ],
    }


class IcebergMaintenanceResource(dg.ConfigurableResource):
    """Snapshot-retention housekeeping for tradinghub_eu's Iceberg tables,
    used by each endpoint component's maintenance job (see
    ``components/tradinghub_eu_market_data_component.py``). Reuses the same
    REST catalog the IOManager writes through.
    """

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

        keep_ids = {s.snapshot_id for s in snapshots[:min_snapshots_to_keep]}
        cutoff_ms = (
            datetime.now(UTC) - timedelta(weeks=max_snapshot_age_weeks)
        ).timestamp() * 1000

        # pyiceberg's expire_snapshots().older_than() has no "keep at least N"
        # concept on its own - it would happily expire every snapshot on a
        # table that hasn't been touched in a while. Compute the exact set to
        # expire ourselves: beyond the min_snapshots_to_keep floor, AND older
        # than the age cutoff.
        to_expire = [
            s.snapshot_id
            for s in snapshots[min_snapshots_to_keep:]
            if s.snapshot_id not in keep_ids and s.timestamp_ms < cutoff_ms
        ]

        if not to_expire:
            context.log.info(
                f"{table_name}: nothing to expire ({len(snapshots)}"
                "snapshot(s) total)"
            )
            return

        table.maintenance.expire_snapshots().by_ids(to_expire).commit()
        context.log.info(
            f"{table_name}: expired {len(to_expire)} snapshot(s), kept"
            f"{len(snapshots) - len(to_expire)}"
        )


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
                catalog_properties=catalog_properties,
                namespace=NAMESPACE,
            ),
        }
    )
