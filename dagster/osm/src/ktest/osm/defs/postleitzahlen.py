import tempfile

import dagster as dg
import numpy as np
import pyarrow as pa
import pyogrio
import shapely

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
        " release; a postcode can span several relations (e.g. 75378)."
        " A few geometries are invalid as delivered, as yetzt's rounding to"
        " 6 decimals and ring assembly break areas that are valid in OSM;"
        " for those only, `wkb_geometry_repaired` holds the geometry"
        " repaired with shapely's make_valid (polygonal parts only)."
    ),
    group_name="osm",
    kinds={"iceberg"},
    partitions_def=postleitzahlen_release_partitions_def,
    metadata={
        "partition_expr": PARTITION_COLUMN,
        "table_properties": {},
        "write_mode": "overwrite",
        # wkb_geometry_repaired was added after the first imports; lets the
        # overwrite add the column to the existing table instead of failing
        "schema_update_mode": "update",
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
    repaired = _repair_invalid(table.column("wkb_geometry"))
    table = table.append_column("wkb_geometry_repaired", repaired)

    return dg.MaterializeResult(
        value=table,
        metadata={
            "row_count": table.num_rows,
            "repaired_geometries": table.num_rows - repaired.null_count,
        },
    )


def _repair_invalid(wkb: pa.ChunkedArray) -> pa.Array:
    """make_valid() result for invalid geometries, null for valid ones.
    Collapsed parts (e.g. a spike reduced to a line) are dropped, so the
    result stays a (Multi)Polygon."""
    geometries = shapely.from_wkb(wkb.to_numpy(zero_copy_only=False))
    invalid = ~shapely.is_valid(geometries)
    repaired = np.full(len(geometries), None, dtype=object)
    repaired[invalid] = shapely.to_wkb(
        shapely.make_valid(
            geometries[invalid], method="structure", keep_collapsed=False
        )
    )
    return pa.array(repaired, type=pa.binary())


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
