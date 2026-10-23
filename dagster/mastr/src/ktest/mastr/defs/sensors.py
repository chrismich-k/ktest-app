import os

import dagster as dg

from ktest.mastr import archive
from ktest.mastr.asset_keys import ALL as ALL_ASSET_KEYS
from ktest.mastr.partitions import mastr_stichtag_partitions_def

RAW_BUCKET = os.environ["MASTR_RAW_S3_BUCKET"]
RAW_PREFIX = os.environ["MASTR_RAW_S3_PREFIX"]

mastr_refresh_job = dg.define_asset_job(
    name="mastr_refresh_job",
    selection=ALL_ASSET_KEYS,
    partitions_def=mastr_stichtag_partitions_def,
)


@dg.sensor(
    name="mastr_archive_sensor",
    job=mastr_refresh_job,
    # Hourly is comfortably frequent even for daily exports (not just
    # quarterly Stichtag snapshots - raw-data/mastr/ gets both). The
    # listing stays cheap regardless - low thousands of objects at most
    # even after years of daily uploads.
    minimum_interval_seconds=3600,
    required_resource_keys={"mastr_raw_s3"},
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def mastr_archive_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    # S3Resource implements IAttachDifferentObjectToOpContext, so accessing
    # it via required_resource_keys (as opposed to a typed resource param)
    # already gives the constructed boto3 client, not the S3Resource
    # wrapper - calling .get_client() again here would fail.
    client = context.resources.mastr_raw_s3
    available = set(
        archive.discover_available_stichtage(client, RAW_BUCKET, RAW_PREFIX)
    )
    known = set(
        mastr_stichtag_partitions_def.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    new_keys = sorted(available - known)
    if not new_keys:
        return dg.SensorResult(
            skip_reason="No new, fully-uploaded mastr stichtag folders."
        )

    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(partition_key=key, run_key=key) for key in new_keys
        ],
        dynamic_partitions_requests=[
            mastr_stichtag_partitions_def.build_add_request(new_keys)
        ],
    )


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        jobs=[mastr_refresh_job], sensors=[mastr_archive_sensor]
    )
