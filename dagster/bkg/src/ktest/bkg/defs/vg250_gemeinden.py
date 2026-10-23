"""BKG VG250 Gemeinde boundaries - raw import only, one asset (no factory
needed, unlike mastr's four). Centroid computation and WGS84 reprojection
are a dbt model in dataware, not done here - raw-data-untouched-derive-in-dbt
convention, same reasoning that kept mastr's Katalogwerte.KatalogKategorieId
an unresolved raw FK.
"""

import os
import shutil
import tempfile

import dagster as dg
import pyarrow as pa
import pyogrio

from ktest.bkg import archive
from ktest.bkg.asset_keys import VG250_GEMEINDEN
from ktest.bkg.partitions import bkg_stichtag_partitions_def

PARTITION_COLUMN = "stichtag"
GEM_LAYER = "vg250_gem"
# ETRS89 / UTM zone 32N - verified via gpkg_spatial_ref_sys against the
# real file, not assumed from the "utm32s" filename alone.
GEM_SRID = 25832

RAW_BUCKET = os.environ["BKG_RAW_S3_BUCKET"]
RAW_PREFIX = os.environ["BKG_RAW_S3_PREFIX"]


@dg.asset(
    key=VG250_GEMEINDEN,
    description=(
        "BKG VG250 Gemeinde boundaries (vg250_gem layer), raw - AGS/GEN/ARS"
        "/BEZ plus the untouched polygon geometry (column 'geom', WKB,"
        " EPSG:25832/UTM32N - verified directly: pyogrio emits real ISO WKB"
        " here, not the GeoPackage-specific binary wrapper the source file"
        " itself uses internally). Centroid computation and WGS84"
        " reprojection are a dbt model in dataware, not done here. Natural"
        " key: AGS, unique only within a single stichtag (full re-export"
        " each year) - unique across the whole table only as"
        " (AGS, stichtag)."
    ),
    group_name="bkg",
    kinds={"iceberg"},
    partitions_def=bkg_stichtag_partitions_def,
    metadata={
        "partition_expr": PARTITION_COLUMN,
        "table_properties": {},
        "write_mode": "overwrite",
    },
    # Global pool, limit=1, configured externally at the Dagster
    # instance/deployment level - prevents Iceberg commit conflicts, same
    # mechanism as smard/tradinghub_eu/mastr's own write pools.
    pool="bkg_iceberg_writes",
    required_resource_keys={"bkg_raw_s3"},
)
def vg250_gemeinden(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    stichtag = context.partition_key
    # S3Resource implements IAttachDifferentObjectToOpContext, so accessing
    # it via required_resource_keys already gives the constructed boto3
    # client, not the S3Resource wrapper - see ktest.mastr's same fix.
    client = context.resources.bkg_raw_s3

    archive_key = archive.discover_archive_key(
        client, RAW_BUCKET, RAW_PREFIX, stichtag
    )
    zf = archive.open_remote_zip(client, RAW_BUCKET, archive_key)
    member_name = archive.find_gpkg_member(zf)

    with zf.open(member_name) as fh:
        # pyogrio needs real file/path-like random access (GeoPackage is a
        # SQLite container, not a stream) - write the one extracted member
        # to a temp file first. Only ~11,000 rows regardless; no mastr-style
        # memory-bounding concern here.
        with tempfile.NamedTemporaryFile(suffix=".gpkg") as tmp:
            shutil.copyfileobj(fh, tmp)
            tmp.flush()
            _, table = pyogrio.raw.read_arrow(
                tmp.name,
                layer=GEM_LAYER,
                columns=["AGS", "GEN", "ARS", "BEZ"],
            )

    table = table.append_column(
        PARTITION_COLUMN,
        pa.array([stichtag] * table.num_rows, type=pa.string()),
    )
    table = table.append_column(
        "srid", pa.array([GEM_SRID] * table.num_rows, type=pa.int32())
    )

    return dg.MaterializeResult(
        value=table,
        metadata={
            "archive_key": archive_key,
            "row_count": table.num_rows,
        },
    )


@dg.op(
    name="bkg_vg250_gemeinden_expire_snapshots_op",
    required_resource_keys={"iceberg_maintenance"},
)
def _expire_snapshots_op(context: dg.OpExecutionContext) -> None:
    context.resources.iceberg_maintenance.expire_snapshots(
        table_name="vg250_gemeinden",
        max_snapshot_age_weeks=12,
        min_snapshots_to_keep=5,
        context=context,
    )


@dg.job(name="bkg_vg250_gemeinden_maintenance_job")
def _maintenance_job():
    _expire_snapshots_op()


_maintenance_schedule = dg.ScheduleDefinition(
    name="bkg_vg250_gemeinden_maintenance_schedule",
    job=_maintenance_job,
    cron_schedule="0 3 * * 0",
    execution_timezone="Europe/Berlin",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[vg250_gemeinden],
        jobs=[_maintenance_job],
        schedules=[_maintenance_schedule],
    )
