import tempfile

import dagster as dg
import pyarrow as pa
import pyogrio

from ktest.osm import postleitzahlen_releases
from ktest.osm.asset_keys import POSTLEITZAHLEN
from ktest.osm.partitions import postleitzahlen_release_partitions_def

PARTITION_COLUMN = "release"
SRID = 4326


@dg.asset(
    key=POSTLEITZAHLEN,
    description=(
        "German postcode areas from OpenStreetMap (boundary=postal_code"
        " relations), raw as published by github.com/yetzt/postleitzahlen:"
        " postcode, OSM relation id `rel` and the polygon geometry"
        " (`wkb_geometry`, WKB, EPSG:4326). License ODbL-1.0, attribution"
        " '© OpenStreetMap contributors'. Natural key: rel, unique only per"
        " release; a postcode can span several relations (e.g. 75378), and"
        " some geometries are invalid (self-intersections) as delivered."
    ),
    group_name="osm",
    kinds={"iceberg"},
    partitions_def=postleitzahlen_release_partitions_def,
    metadata={
        "partition_expr": PARTITION_COLUMN,
        "table_properties": {},
        "write_mode": "overwrite",
    },
    pool="osm_iceberg_writes",
)
def postleitzahlen(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    release = context.partition_key

    # GDAL needs a real file; the decompressed GeoJSON is about 0.5 GB.
    with tempfile.NamedTemporaryFile(suffix=".geojson") as tmp:
        postleitzahlen_releases.download_geojson(release, tmp)
        tmp.flush()
        _, table = pyogrio.raw.read_arrow(
            tmp.name, columns=["postcode", "rel"]
        )

    table = table.append_column(
        PARTITION_COLUMN,
        pa.array([release] * table.num_rows, type=pa.string()),
    )
    table = table.append_column(
        "srid", pa.array([SRID] * table.num_rows, type=pa.int32())
    )

    return dg.MaterializeResult(
        value=table,
        metadata={"row_count": table.num_rows},
    )


@dg.op(
    name="osm_postleitzahlen_expire_snapshots_op",
    required_resource_keys={"iceberg_maintenance"},
)
def _expire_snapshots_op(context: dg.OpExecutionContext) -> None:
    context.resources.iceberg_maintenance.expire_snapshots(
        table_name="postleitzahlen",
        max_snapshot_age_weeks=12,
        min_snapshots_to_keep=5,
        context=context,
    )


@dg.job(name="osm_postleitzahlen_maintenance_job")
def _maintenance_job():
    _expire_snapshots_op()


_maintenance_schedule = dg.ScheduleDefinition(
    name="osm_postleitzahlen_maintenance_schedule",
    job=_maintenance_job,
    cron_schedule="0 3 * * 0",
    execution_timezone="Europe/Berlin",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[postleitzahlen],
        jobs=[_maintenance_job],
        schedules=[_maintenance_schedule],
    )
