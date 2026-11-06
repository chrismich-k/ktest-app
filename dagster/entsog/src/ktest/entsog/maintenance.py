import dagster as dg


def maintenance_defs(
    name: str, table_name: str, timezone: str
) -> tuple[dg.JobDefinition, dg.ScheduleDefinition]:
    """Job and weekly schedule expiring the table's old snapshots."""

    @dg.op(
        name=f"{name}_expire_snapshots_op",
        required_resource_keys={"entsog_iceberg"},
    )
    def _expire_snapshots_op(context: dg.OpExecutionContext) -> None:
        context.resources.entsog_iceberg.expire_snapshots(
            table_name=table_name,
            max_snapshot_age_weeks=4,
            min_snapshots_to_keep=5,
            context=context,
        )

    @dg.job(name=f"{name}_maintenance_job")
    def _maintenance_job():
        _expire_snapshots_op()

    schedule = dg.ScheduleDefinition(
        name=f"{name}_maintenance_schedule",
        job=_maintenance_job,
        cron_schedule="0 3 * * 0",
        execution_timezone=timezone,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    return _maintenance_job, schedule
