"""Shared factory: one raw MaStR asset (+ its own snapshot-retention
maintenance op/job/schedule) per archive object type. Every instance reads
the same stichtag's zip and extracts only its own member file(s) - see
archive.py for why this never downloads the whole ~3GB archive. The 4 raw
assets are siblings, not a DAG among themselves - no deps= between e.g.
Katalogwerte and Katalogkategorien, even though Katalogwerte.KatalogKategorieId
is a real FK: FK resolution is explicitly deferred to dbt.
"""

import os

import dagster as dg
import pyarrow as pa

from ktest.mastr import archive
from ktest.mastr.partitions import mastr_stichtag_partitions_def
from ktest.mastr.xml_rows import FieldSpec, parse_rows

PARTITION_COLUMN = "stichtag"

RAW_BUCKET = os.environ["MASTR_RAW_S3_BUCKET"]
RAW_PREFIX = os.environ["MASTR_RAW_S3_PREFIX"]


def build_raw_archive_asset(
    *,
    key: dg.AssetKey,
    table_name: str,
    row_tag: str,
    file_prefix: str,
    field_map: list[FieldSpec],
    description: str,
    maintenance_cron: str = "0 3 * * 0",
) -> dg.Definitions:
    @dg.asset(
        key=key,
        description=description,
        group_name="mastr",
        kinds={"iceberg"},
        partitions_def=mastr_stichtag_partitions_def,
        metadata={
            "partition_expr": PARTITION_COLUMN,
            "table_properties": {},
            "write_mode": "overwrite",
        },
        # Global pool, limit=1, configured externally at the Dagster
        # instance/deployment level - prevents Iceberg commit conflicts
        # across all 4 tables (same mechanism as smard_iceberg_writes).
        pool="mastr_iceberg_writes",
        required_resource_keys={"mastr_raw_s3"},
    )
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        stichtag = context.partition_key
        # S3Resource implements IAttachDifferentObjectToOpContext, so
        # accessing it via required_resource_keys already gives the
        # constructed boto3 client, not the S3Resource wrapper - see
        # defs/sensors.py for the same pattern.
        client = context.resources.mastr_raw_s3

        archive_key = archive.discover_archive_key(
            client, RAW_BUCKET, RAW_PREFIX, stichtag
        )
        schema_version = archive.extract_schema_version(archive_key)
        zf = archive.open_remote_zip(client, RAW_BUCKET, archive_key)
        member_names = archive.find_member_entries(zf, file_prefix)

        tables = []
        missing_required: dict[str, int] = {}
        for name in member_names:
            with zf.open(name) as fh:
                part_table, part_missing = parse_rows(fh, row_tag, field_map)
            tables.append(part_table)
            for tag, count in part_missing.items():
                missing_required[tag] = missing_required.get(tag, 0) + count
        table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

        # MUST be string, not date32/timestamp - DynamicPartitionsDefinition
        # is not a TimeWindowPartitionsDefinition, so dagster_iceberg treats
        # this as a "static" partition dimension and requires a StringType
        # Iceberg column. A date32 column here raises ValueError on write.
        table = table.append_column(
            PARTITION_COLUMN,
            pa.array([stichtag] * table.num_rows, type=pa.string()),
        )
        # Lets dbt (or a human) know which export revision a row came from
        # - parsed from the archive's own filename, no version marker
        # exists inside the XML itself.
        table = table.append_column(
            "schema_version",
            pa.array([schema_version] * table.num_rows, type=pa.string()),
        )

        if missing_required:
            context.log.warning(
                f"{table_name}/{stichtag} (schema {schema_version}): required"
                f" fields missing on some rows - {missing_required}. Can"
                " happen on an older/differently-structured export, or on"
                " a genuine data gap in the registry itself - not"
                " necessarily a bug."
            )

        return dg.MaterializeResult(
            value=table,
            metadata={
                "archive_key": archive_key,
                "schema_version": schema_version,
                "members_read": member_names,
                "row_count": table.num_rows,
                "missing_required_fields": missing_required,
            },
        )

    @dg.op(
        name=f"mastr_{table_name.lower()}_expire_snapshots_op",
        required_resource_keys={"iceberg_maintenance"},
    )
    def _expire_snapshots_op(context: dg.OpExecutionContext) -> None:
        context.resources.iceberg_maintenance.expire_snapshots(
            table_name=table_name,
            max_snapshot_age_weeks=12,
            min_snapshots_to_keep=5,
            context=context,
        )

    @dg.job(name=f"mastr_{table_name.lower()}_maintenance_job")
    def _maintenance_job():
        _expire_snapshots_op()

    _maintenance_schedule = dg.ScheduleDefinition(
        name=f"mastr_{table_name.lower()}_maintenance_schedule",
        job=_maintenance_job,
        cron_schedule=maintenance_cron,
        execution_timezone="Europe/Berlin",
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )

    return dg.Definitions(
        assets=[_asset],
        jobs=[_maintenance_job],
        schedules=[_maintenance_schedule],
    )
