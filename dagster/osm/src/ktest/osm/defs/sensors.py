import dagster as dg

from ktest.osm import postleitzahlen_releases
from ktest.osm.asset_keys import POSTLEITZAHLEN
from ktest.osm.partitions import postleitzahlen_release_partitions_def

postleitzahlen_refresh_job = dg.define_asset_job(
    name="osm_postleitzahlen_refresh_job",
    selection=[POSTLEITZAHLEN],
    partitions_def=postleitzahlen_release_partitions_def,
)


@dg.sensor(
    name="osm_postleitzahlen_release_sensor",
    job=postleitzahlen_refresh_job,
    minimum_interval_seconds=7 * 86400,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def postleitzahlen_release_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    available = set(postleitzahlen_releases.list_release_tags())
    known = set(
        postleitzahlen_release_partitions_def.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    new_tags = sorted(available - known)
    if not new_tags:
        return dg.SensorResult(skip_reason="No new postleitzahlen release.")
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(partition_key=tag, run_key=tag) for tag in new_tags
        ],
        dynamic_partitions_requests=[
            postleitzahlen_release_partitions_def.build_add_request(new_tags)
        ],
    )


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        jobs=[postleitzahlen_refresh_job],
        sensors=[postleitzahlen_release_sensor],
    )
