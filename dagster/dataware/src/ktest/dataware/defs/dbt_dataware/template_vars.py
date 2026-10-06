import dagster as dg


@dg.template_var
def rebuild_on_upstream_update() -> dg.AutomationCondition:
    """Rebuild as soon as an upstream import (e.g. any partition of
    tradinghub_eu/ExternalBalancingGas) or upstream dbt model updated.

    eager() would also wait until no upstream partition is missing at all
    (future lookahead days, failed days); for a full rebuild missing
    partitions are harmless, so that guard is dropped.
    """
    return (
        dg.AutomationCondition.eager()
        .without(~dg.AutomationCondition.any_deps_missing())
        .with_label("eager_allow_missing")
    )
