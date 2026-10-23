import os

import dagster as dg

from ktest.bkg import archive
from ktest.bkg.asset_keys import ALL as ALL_ASSET_KEYS
from ktest.bkg.partitions import bkg_stichtag_partitions_def

RAW_BUCKET = os.environ["BKG_RAW_S3_BUCKET"]
RAW_PREFIX = os.environ["BKG_RAW_S3_PREFIX"]

bkg_refresh_job = dg.define_asset_job(
    name="bkg_refresh_job",
    selection=ALL_ASSET_KEYS,
    partitions_def=bkg_stichtag_partitions_def,
)


@dg.sensor(
    name="bkg_archive_sensor",
    job=bkg_refresh_job,
    # VG250 is published roughly yearly, not daily like mastr's archive -
    # a daily poll is already far more frequent than needed, the listing
    # stays cheap regardless.
    minimum_interval_seconds=86400,
    required_resource_keys={"bkg_raw_s3"},
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def bkg_archive_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    # S3Resource implements IAttachDifferentObjectToOpContext, so accessing
    # it via required_resource_keys already gives the constructed boto3
    # client, not the S3Resource wrapper - see ktest.mastr's same fix.
    client = context.resources.bkg_raw_s3
    available = set(
        archive.discover_available_stichtage(client, RAW_BUCKET, RAW_PREFIX)
    )
    known = set(
        bkg_stichtag_partitions_def.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    new_keys = sorted(available - known)
    if not new_keys:
        return dg.SensorResult(
            skip_reason="No new, fully-uploaded bkg stichtag folders."
        )

    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(partition_key=key, run_key=key) for key in new_keys
        ],
        dynamic_partitions_requests=[
            bkg_stichtag_partitions_def.build_add_request(new_keys)
        ],
    )


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(jobs=[bkg_refresh_job], sensors=[bkg_archive_sensor])
