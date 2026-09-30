from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any, Literal

import dagster as dg
import httpx
import pyarrow as pa
import pyarrow.compute as pc

from ktest.tradinghub_eu.http import TheApiError
from ktest.tradinghub_eu.http import get as http_get
from ktest.tradinghub_eu.xsd import fetch_xsd_schema, rows_to_arrow_table


class TheDateParamMode(StrEnum):
    RANGE = "range"
    SINGLE = "single"
    NONE = "none"


class TheIcebergRetentionConfig(dg.Model, dg.Resolvable):
    max_snapshot_age_weeks: int = 12
    min_snapshots_to_keep: int = 5


class TheIcebergConfig(dg.Model, dg.Resolvable):
    table_properties: dict[str, str] = {}
    retention: TheIcebergRetentionConfig = TheIcebergRetentionConfig()


def _seconds_to_cron(seconds: int) -> str:
    """Convert a refresh interval in seconds to a cron expression. Only
    whole-minute, whole-hour (evenly dividing a day), or exactly-one-day
    cadences are supported - that covers every realistic refresh interval
    for this API; anything else fails loudly at component-load time rather
    than silently rounding.
    """
    if seconds == 86400:
        return "0 0 * * *"
    if seconds % 3600 == 0:
        hours = seconds // 3600
        if 24 % hours != 0:
            raise ValueError(
                f"refresh_seconds={seconds} ({hours}h) must evenly divide 24h"
            )
        return f"0 */{hours} * * *"
    if seconds % 60 == 0:
        minutes = seconds // 60
        if minutes < 1 or 60 % minutes != 0:
            raise ValueError(
                f"refresh_seconds={seconds} ({minutes}m) must evenly divide60m"
            )
        return f"*/{minutes} * * * *"
    raise ValueError(
        f"refresh_seconds={seconds} must be a whole number of minutes, hours,"
        "or exactly one day"
    )


def build_params(
    component: "TradinghubEuMarketDataComponent", start: str, end: str
) -> dict[str, str]:
    """Build THE query params for a Dagster partition-key range. For a
    normal single-partition run, ``start == end``, identical to today's
    per-day request; for a batched backfill run (``BackfillPolicy.multi_run``),
    they span the whole batch in one request.
    """
    params = dict(component.extra_params)
    if component.date_param_mode == TheDateParamMode.RANGE:
        params[component.start_date_param] = start
        params[component.end_date_param] = end
    elif component.date_param_mode == TheDateParamMode.SINGLE:
        params[component.single_date_param] = start
    return params


class TradinghubEuMarketDataComponent(dg.Component, dg.Model, dg.Resolvable):
    """Downloads one Trading Hub Europe DataExport API endpoint's data into a
    single Iceberg table, partitioned daily or monthly by a synthesized
    partition column. The Arrow/Iceberg schema is derived dynamically from
    the endpoint's own XSD (see ``xsd.py``) rather than hardcoded, so this one
    component class covers all ~50 published endpoints - see
    ``defs/tradinghub_eu_market_data/<Endpoint>/defs.yaml`` for one instance
    per endpoint.

    See https://api.tradinghub.eu/api/dataexport/manual/de for the full API
    manual this component's behavior is derived from.
    """

    endpoint: str
    start_date: str
    date_param_mode: TheDateParamMode = TheDateParamMode.RANGE
    start_date_param: str = "startDate"
    end_date_param: str = "endDate"
    single_date_param: str = "anzeigeDatum"
    extra_params: dict[str, str] = {}
    rate_limit_class: Literal["standard", "erhoeht"] = "standard"
    user_agent_string: str
    sort_columns: list[str]
    refresh_seconds: int = 14400
    maintenance_cron: str = "0 3 * * 0"
    iceberg: TheIcebergConfig = TheIcebergConfig()
    # dagster_iceberg's IOManager write mode.
    # "overwrite" (default) replaces whatever's already in the target
    # partition - the right choice for almost every endpoint, since a
    # re-materialized partition is meant to *replace* stale/preliminary data
    # with the latest complete answer.
    #
    # "append" is only for "live-crawl" assets where THE returns just a single
    # current-moment snapshot with no way to ask for history
    # (date_param_mode="none") and every materialization is meant to add one
    # more version rather than replace the last one - e.g.
    # AggregateImbalancePositions, which only reports the *current* hour's
    # value, so a refresh every hour with iceberg_write_mode="append" builds
    # up an hourly history that "overwrite" would otherwise destroy on every
    # run. Only safe/meaningful when each row already carries a real
    # distinguishing timestamp of its own - dagster_iceberg's append mode does
    # no deduplication at all, so without that, appended rows for the same
    # logical fact would be indistinguishable from each other. There's no
    # synthetic "fetched_at"/"insert_date" column injected here - not needed
    # for the one endpoint this is used on today, but would be worth adding if
    # a future endpoint needs "append" without such a field of its own.
    #
    # "upsert" - a real MERGE: rows matching iceberg_upsert_join_columns get
    # updated in place, genuinely new rows get inserted, and rows *absent*
    # from the current fetch are left untouched entirely. No filter/deletion
    # step exists at all in this path, so unlike "overwrite" it's safe to use
    # fully unpartitioned even for an endpoint whose response only ever covers
    # a *subset* of history (e.g. PricesFlexibilityChargeIntraday's
    # "hours since this gas day began" - with "overwrite" + unpartitioned that
    # would wipe every earlier day's data the moment the gas day rolls over,
    # since unpartitioned "overwrite" replaces the *entire* table; "upsert"
    # only ever touches rows for hours actually present in the response,
    # leaving prior days alone). Also naturally handles THE's
    # preliminary-to-final refinement within a day (a repeated Timestamp
    # gets its values updated, not duplicated) without needing "append"'s
    # create_ts/no-deduplication trade-off. Requires
    # iceberg_upsert_join_columns.
    iceberg_write_mode: Literal["overwrite", "append", "upsert"] = "overwrite"
    # Required (and only meaningful) when iceberg_write_mode == "upsert":
    # the column(s) that uniquely identify "the same logical row" across
    # fetches - passed straight through as UpsertOptions.join_cols.
    # when_matched_update_all/when_not_matched_insert_all are left at
    # dagster_iceberg's defaults (both True: always update on match,
    # always insert on no match) - no known case here needs anything else.
    iceberg_upsert_join_columns: list[str] = []
    # Only meaningful for date_param_mode == "range": how many contiguous
    # daily partitions a single backfill run may cover in one THE request
    # (THE allows arbitrary date ranges per request for most endpoints, so
    # batching cuts the number of requests/HTTP round-trips during a
    # backfill). Restricted to {1, 7}, not an arbitrary int:
    # dagster_iceberg==0.3.14 derives the Iceberg partition-spec transform
    # from the *current write's* time window on every single write (not just
    # once at table creation), and only recognizes exactly 1 hour, 24 hours,
    # 168 hours (1 week - hardcoded as a DayTransform special case), or an
    # exact aligned calendar month which unfortunately Dagster's range
    # validation can't verify or enforce, so not used here.
    backfill_max_partitions_per_run: Literal[1, 7] = 1
    # Workaround for a confirmed THE-side quirk (not documented, found live
    # 2026-09-29 against ExternalBalancingGas): the day(s) right at a
    # request's endDate are often missing from the response even though
    # they're fully in the past and have data - extending endDate further
    # forward reveals them. E.g. endDate=2026-09-28 alone omits 09-28
    # entirely; endDate=2026-09-29 includes it. When set > 0, the asset
    # requests `end_date_lookahead_days` extra days past its own partition
    # range, then discards rows whose derived the partition column falls
    # outside that range before writing (they belong to other partitions, which
    # will pick them up via their own runs) - so it never writes outside the
    # partition(s) this run actually owns. Only meaningful for date_param_mode
    # == "range". Opt-in per endpoint (default 0/off) since only
    # ExternalBalancingGas has been confirmed to need it so far.
    end_date_lookahead_days: int = 0
    # Some endpoints aren't really a time series at all - e.g. FeesAndLevies
    # returns its *entire* history unconditionally on every fetch, so
    # partitioning would make no sense here. When False, this builds a single
    # unpartitioned asset instead: no partitions_def/backfill_policy, no
    # per-run date params, no partition-column partition-scoping - THE's full
    # response is written as-is. Dagster_iceberg's default `overwrite` mode
    # replaces the whole table on every refresh, so `append` or `upsert` could
    # make sense in some cases. Requires date_param_mode="none", and disables
    # backfill_max_partitions_per_run/end_date_lookahead_days which are
    # partitioning concepts that are meaningless here.
    partitioned: bool = True
    # Some endpoints only ever publish monthly-granularity rows, so "monthly"
    # uses dg.MonthlyPartitionsDefinition which comes out as e.g.
    # "2026-09-01",matching THE's own Month field convention exactly, so a
    # plain single-day-shaped request (start=end=partition key) already lines
    # up with THE's month-matching. Also plays nicer with dagster_iceberg's
    # partition-transform derivation than daily batching does: a
    # single-partition monthly write is always an exact 1-calendar-month
    # window, which is one of the few window sizes diff_to_transformation
    # actually supports (alongside 1h/24h/168h) - see
    # backfill_max_partitions_per_run's docstring above. Requires
    # date_param_mode="range" and disables batching/lookahead.
    partition_granularity: Literal["daily", "monthly"] = "daily"
    # How many extra (not-yet-fully-elapsed) periods beyond "now" Dagster
    # treats as valid/requestable partition keys - passed straight through
    # to partitions_def's own end_offset. None (default) picks a granularity-
    # aware default rather than Dagster's own flat 0: "daily" defaults to 1,
    # "monthly" defaults to 0 (Dagster's own standard - most monthly-shaped
    # endpoints never have data for the current, still-forming month anyways).
    # Override to 1 explicitly for a monthly-partitioned endpoint that
    # actually contains live daily-grained data (i.e. was converted to
    # "monthly" purely to reduce backfill/storage overhead, not because the
    # data itself is month-lagged) - in that case, this month's data is worth
    # refreshing before the month is over.
    dagster_partition_end_offset: int | None = None
    spec: dg.ResolvedAssetSpec

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.partition_granularity == "monthly":
            if self.date_param_mode != TheDateParamMode.RANGE:
                raise ValueError(
                    f"{self.endpoint}: partition_granularity='monthly'"
                    "requires date_param_mode='range' (got"
                    f"{self.date_param_mode.value!r})."
                )
            if (
                self.backfill_max_partitions_per_run != 1
                or self.end_date_lookahead_days != 0
            ):
                raise ValueError(
                    f"{self.endpoint}: partition_granularity='monthly' is"
                    "incompatible with"
                    "backfill_max_partitions_per_run/end_date_lookahead_days"
                    "(both are day-window concepts; dagster_iceberg also"
                    "can't batch multiple months into one write - see"
                    "backfill_max_partitions_per_run's docstring)."
                )
        if (
            self.date_param_mode != TheDateParamMode.RANGE
            and self.backfill_max_partitions_per_run > 1
        ):
            raise ValueError(
                f"{self.endpoint}: backfill_max_partitions_per_run > 1"
                "requires date_param_mode='range' (got"
                f"{self.date_param_mode.value!r}) - a 'single'/'none' endpoint"
                "doesn't accept a date *range* request, so batching multiple"
                "partitions into one run isn't meaningful for it."
            )
        if (
            self.date_param_mode != TheDateParamMode.RANGE
            and self.end_date_lookahead_days > 0
        ):
            raise ValueError(
                f"{self.endpoint}: end_date_lookahead_days > 0 requires "
                "date_param_mode='range' (got"
                f"{self.date_param_mode.value!r})."
            )
        if (
            self.iceberg_write_mode == "upsert"
            and not self.iceberg_upsert_join_columns
        ):
            raise ValueError(
                f"{self.endpoint}: iceberg_write_mode='upsert' requires "
                "iceberg_upsert_join_columns to be set (the column(s) that "
                "uniquely identify one logical row across fetches)."
            )
        if not self.partitioned:
            if self.date_param_mode != TheDateParamMode.NONE:
                raise ValueError(
                    f"{self.endpoint}: partitioned=False requires"
                    "date_param_mode='none' (got"
                    f"{self.date_param_mode.value!r}) - an unpartitioned table"
                    "has no per-run date to request a range/single date for."
                )
            if (
                self.backfill_max_partitions_per_run != 1
                or self.end_date_lookahead_days != 0
            ):
                raise ValueError(
                    f"{self.endpoint}: partitioned=False is incompatible with "
                    "backfill_max_partitions_per_run/end_date_lookahead_days"
                    "(both are partitioning concepts)."
                )
        # Fail fast on a bad refresh_seconds at component-load time, not at
        # schedule-tick time.
        refresh_cron = _seconds_to_cron(self.refresh_seconds)

        # Named after the *mechanism* (which Dagster partition granularity
        # owns this row), not a date from the THE domain. THE's own date-typed
        # field is always kept verbatim in its own separate column.
        partition_column = f"dg_{self.partition_granularity}_partition"

        asset_kwargs: dict[str, Any] = dict(
            key=self.spec.key,
            description=self.spec.description,
            group_name=self.spec.group_name,
            kinds=self.spec.kinds,
            metadata={
                **self.spec.metadata,
                "table_properties": self.iceberg.table_properties,
                "rate_limit_class": self.rate_limit_class,
                "write_mode": self.iceberg_write_mode,
                **(
                    {
                        "upsert_options": {
                            "join_cols": self.iceberg_upsert_join_columns
                        }
                    }
                    if self.iceberg_write_mode == "upsert"
                    else {}
                ),
            },
            # Global pool (not per-table) - THE explicitly asks clients to
            # run requests sequentially, never in parallel; this satisfies
            # that across all ~50 endpoints at once and, as a free side
            # effect, also fully prevents Iceberg commit conflicts across
            # all ~50 tables without needing a per-table pool. Concurrency
            # limit (1) is configured externally at the Dagster instance/
            # deployment level, same as smard's `smard_iceberg_writes`.
            # Note that this needs to be set up in Dagster UI / Deployment /
            # Concurrenty / pools to make the feature work!
            pool="tradinghub_eu_api_requests",
        )
        # end_offset=1: by default a partition only becomes a *valid* key
        # once its window has fully elapsed (Dagster's partitions model
        # assumes a time window's data doesn't exist until the window
        # closes) - "today"/"this month" wouldn't even be a requestable
        # partition key otherwise, which directly conflicts with THE's own
        # model where the current gas day/month is real but provisional and
        # meant to be re-fetched as it fills in (manual chapter 4.4). This
        # makes the current period valid one window early, matching that.
        if not self.partitioned:
            partitions_def = None
        else:
            end_offset = self.dagster_partition_end_offset
            if end_offset is None:
                end_offset = (
                    0 if self.partition_granularity == "monthly" else 1
                )
            if self.partition_granularity == "monthly":
                partitions_def = dg.MonthlyPartitionsDefinition(
                    start_date=self.start_date, end_offset=end_offset
                )
            else:
                partitions_def = dg.DailyPartitionsDefinition(
                    start_date=self.start_date, end_offset=end_offset
                )
        if self.partitioned:
            asset_kwargs["metadata"]["partition_expr"] = partition_column
            asset_kwargs["partitions_def"] = partitions_def
            asset_kwargs["backfill_policy"] = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_max_partitions_per_run
            )

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if self.partitioned:
                key_range = context.partition_key_range
                start = date.fromisoformat(key_range.start)
                end = date.fromisoformat(key_range.end)

                if self.date_param_mode == TheDateParamMode.NONE:
                    # This endpoint takes no date parameter at all - THE
                    # always returns whatever is current *right now*.
                    # Preferrably, assets that query those `live-data`
                    # endpoints wouldn't even use a dagster partition (see
                    # AggregateImbalancePositions, MarketAreaBalanceStatus and
                    # PricesFlexibilityChargeIntraday).
                    # But since this asset configuration is actually using a
                    # partition: Materializing anything other than today's
                    # partition would not only fetch nothing - since writes
                    # are per-partition overwrites, it would silently DESTROY
                    # that partition's previously captured actual data by
                    # replacing it with today's data filtered down to nothing.
                    # Dagster's own recommended pattern for this exact
                    # situation: refuse outright rather than allow a
                    # backfill/manual rerun to quietly corrupt history.
                    today_key = partitions_def.get_partition_key_for_timestamp(
                        datetime.now(UTC).timestamp()
                    )
                    if (
                        key_range.start != today_key
                        or key_range.start != key_range.end
                    ):
                        raise Exception(
                            f"{self.endpoint}: this endpoint only ever "
                            f"returns THE's *current* gas day (it accepts "
                            f"no date parameter), so only today's "
                            f"partition ({today_key}) may be materialized "
                            f"- got {key_range.start}..{key_range.end}. "
                            f"Materializing any other partition would "
                            f"silently overwrite it with empty/wrong data."
                        )

                if (
                    start == end
                    and self.date_param_mode == TheDateParamMode.RANGE
                ):
                    # explicitly request the whole date range by computing a
                    # valid.
                    window_end = partitions_def.time_window_for_partition_key(
                        key_range.start
                    ).end
                    request_end = (
                        window_end.date() - timedelta(days=1)
                    ).isoformat()
                else:
                    request_end = key_range.end

                if (
                    self.date_param_mode == TheDateParamMode.RANGE
                    and self.end_date_lookahead_days
                ):
                    request_end = (
                        date.fromisoformat(request_end)
                        + timedelta(days=self.end_date_lookahead_days)
                    ).isoformat()
                params = build_params(self, key_range.start, request_end)
                fallback_gas_day = start
                # Whether this run fetched strictly more than a single
                # partition range (a batched multi-day backfill, or a
                # lookahead-extended request) - evaluated later for filtering.
                over_fetched = (start != end) or bool(
                    self.end_date_lookahead_days
                )
            else:
                # no date_param_mode set / unused case
                start = end = None
                params = build_params(
                    self, "", ""
                )
                # unpartitioned; no partition to fall back to
                fallback_gas_day = None

            schema = fetch_xsd_schema(
                self.endpoint, self.user_agent_string, context
            )

            with httpx.Client() as client:
                try:
                    response = http_get(
                        client,
                        f"/{self.endpoint}",
                        params=params,
                        user_agent=self.user_agent_string,
                        context=context,
                    )
                except TheApiError as e:
                    if (
                        self.partitioned
                        and e.problem.code == "date-before-earliest"
                    ):
                        context.log.info(
                            f"{self.endpoint}: requested range starts "
                            f"before this series' publication cutoff "
                            f"({e.problem.detail}) - writing an empty "
                            f"partition instead of failing the run."
                        )
                        arrays = {
                            f.name: pa.array([], type=f.pa_type)
                            for f in schema.fields
                        }
                        arrays[partition_column] = pa.array(
                            [], type=pa.date32()
                        )
                        return dg.MaterializeResult(
                            value=pa.table(arrays), metadata={"row_count": 0}
                        )
                    raise

            table = rows_to_arrow_table(
                response.content,
                schema,
                sort_columns=self.sort_columns,
                fallback_gas_day=fallback_gas_day,
                partition_column=partition_column,
            )

            if self.iceberg_write_mode == "append":
                # add timestamp suitable for deduplication or joining purposes
                table = table.append_column(
                    "create_ts",
                    pa.array(
                        [datetime.now(UTC)] * table.num_rows,
                        type=pa.timestamp("us", tz="UTC"),
                    ),
                )

            if not self.partitioned:
                # No partition ownership to enforce - THE's full response
                # (whatever it is) replaces the whole table on every refresh
                # (dagster_iceberg's default overwrite mode, unpartitioned).
                return dg.MaterializeResult(
                    value=table, metadata={"row_count": table.num_rows}
                )

            if self.iceberg_write_mode == "append":
                # The entire reason to scope a write down to exactly the
                # partition(s) this run owns (below) is to protect
                # "overwrite" mode's destructive filter-then-replace
                # semantics - a write spanning many partition values broke
                # the underlying Iceberg/S3 write outright under
                # "overwrite" (confirmed live against FeesAndLevies). Append
                # mode doesn't use that filter at all (dagster_iceberg's
                # append_to_table calls a plain, multi-partition-safe
                # table.append()), so there's nothing to protect against by
                # *discarding* rows - keep every row THE returned (e.g.
                # AggregateImbalancePositions' "next gas day" preview row).
                #
                # But a single-partition run (start == end - always true for
                # "none"-mode, since it can never be batched) still only
                # ever *owns* that one Dagster partition, and rows_to_arrow_
                # table's per-row derivation would otherwise tag that
                # preview row with *its own* Gasday date instead - writing
                # into a partition this run was never asked to materialize
                # (confirmed live: today's run tagging a row
                # dg_daily_partition=tomorrow, a full day before any run
                # ever targets that partition key). Pin it to the partition
                # this run actually owns instead, exactly like the "trust"
                # path below does for range/single mode - the row's real
                # timestamp is unaffected, it stays intact in `Gasday`.
                # Only a genuine multi-partition batch (start != end) still
                # needs the per-row values to split rows across their
                # correct partitions - not reachable today for any
                # append-mode endpoint, but kept correct for one that might
                # combine append with batching later.
                if start == end:
                    table = table.drop_columns(
                        [partition_column]
                    ).append_column(
                        partition_column,
                        pa.array([start] * table.num_rows, type=pa.date32()),
                    )
                return dg.MaterializeResult(
                    value=table, metadata={"row_count": table.num_rows}
                )

            if (
                self.date_param_mode
                in (TheDateParamMode.RANGE, TheDateParamMode.SINGLE)
                and not over_fetched
            ):
                # Trust THE's own server-side date scoping instead of
                # re-deriving the partition column from a heuristically-picked
                # date field - confirmed live that this heuristic actively
                # loses real data for several endpoints whose date field
                # doesn't mean "which gas day this row belongs to" the way e.g.
                # Gasday does:
                # BalancingGasContractedCapacity/UmmBalancingContract return
                # rows matched by an *overlapping validity window* (e.g. a
                # contract with ContractRuntimeBegin=yesterday whose
                # ContractRuntimeEnd falls in today's gas day legitimately
                # shows up in today's response, but was being discarded because
                # its Begin field didn't equal today);
                # MarketAreaBalanceStatus's `anzeigeDatum` returns a display
                # snapshot whose own `Gasday` field can be several days older
                # than the date requested. In all these cases THE already
                # answered "what belongs to this partition" - we only need
                # per-row-derived partition values when *we* deliberately
                # fetched beyond our own partition range (batched backfills,
                # lookahead) and must split the extra data back out, or for
                # "none"-mode endpoints with no server-side date scoping at all
                # (handled below).
                table = table.drop_columns([partition_column]).append_column(
                    partition_column,
                    pa.array([start] * table.num_rows, type=pa.date32()),
                )
                return dg.MaterializeResult(
                    value=table,
                    metadata={
                        "partition_range": f"{start}..{end}",
                        "row_count": table.num_rows,
                    },
                )

            # filter rows valid for output of Batched/lookahead range fetches,
            # and "none"-mode endpoints.
            fetched_rows = table.num_rows
            table = table.filter(
                (pc.field(partition_column) >= start)
                & (pc.field(partition_column) <= end)
            )

            metadata = {
                "partition_range": f"{start}..{end}",
                "row_count": table.num_rows,
                "rows_discarded_outside_partition": fetched_rows
                - table.num_rows,
            }
            if (
                self.date_param_mode == TheDateParamMode.RANGE
                and self.end_date_lookahead_days
            ):
                metadata["fetched_range"] = f"{start}..{request_end}"
            return dg.MaterializeResult(value=table, metadata=metadata)

        refresh_job = dg.define_asset_job(
            name=f"tradinghub_eu_{self.endpoint.lower()}_refresh_job",
            selection=[self.spec.key],
            partitions_def=partitions_def,
        )

        if self.partitioned:
            # create a schedule to get the partition that's matching the
            # current timestamp, an create a RunRequest
            @dg.schedule(
                name=f"tradinghub_eu_{self.endpoint.lower()}_refresh_schedule",
                cron_schedule=refresh_cron,
                job=refresh_job,
                execution_timezone="Europe/Berlin",
                default_status=dg.DefaultScheduleStatus.RUNNING,
            )
            def refresh_schedule(
                context: dg.ScheduleEvaluationContext,
            ) -> dg.RunRequest:
                now = context.scheduled_execution_time
                partition_key = partitions_def.get_partition_key_for_timestamp(
                    now.timestamp()
                )
                # run_key must be unique per run, not per partition - this
                # schedule may re-request the same partition many times a day,
                # so create a unique key
                return refresh_job.run_request_for_partition(
                    partition_key=partition_key,
                    run_key=f"{partition_key}_{now.strftime('%Y%m%dT%H%M%S')}",
                    current_time=now,
                )
        else:
            refresh_schedule = dg.ScheduleDefinition(
                name=f"tradinghub_eu_{self.endpoint.lower()}_refresh_schedule",
                job=refresh_job,
                cron_schedule=refresh_cron,
                execution_timezone="Europe/Berlin",
                default_status=dg.DefaultScheduleStatus.RUNNING,
            )

        maintenance_job = self._build_maintenance_job()
        maintenance_schedule = dg.ScheduleDefinition(
            name=f"tradinghub_eu_{self.endpoint.lower()}_maintenance_schedule",
            job=maintenance_job,
            cron_schedule=self.maintenance_cron,
            execution_timezone="Europe/Berlin",
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )

        return dg.Definitions(
            assets=[_asset],
            jobs=[refresh_job, maintenance_job],
            schedules=[refresh_schedule, maintenance_schedule],
        )

    def _build_maintenance_job(self) -> dg.JobDefinition:
        """Snapshot-expiry housekeeping for this endpoint's table, modeled as
        a plain op/job rather than an asset - it doesn't produce a new data
        version, it just prunes old Iceberg snapshots, so it shouldn't
        pollute the asset's materialization/freshness history. Kept
        self-contained per component instance (one job per endpoint) rather
        than one shared job iterating a static table list, since components
        can't see each other's resolved config at build_defs time - this
        keeps each defs.yaml independently deletable, same as its asset.
        """
        endpoint = self.endpoint
        retention = self.iceberg.retention

        @dg.op(
            name=f"tradinghub_eu_{endpoint.lower()}_expire_snapshots_op",
            required_resource_keys={"iceberg_maintenance"},
        )
        def _expire_snapshots_op(context: dg.OpExecutionContext) -> None:
            # explicit `-> None` tells Dagster this op has no output at all,
            # skipping IO-manager handling entirely
            context.resources.iceberg_maintenance.expire_snapshots(
                table_name=endpoint,
                max_snapshot_age_weeks=retention.max_snapshot_age_weeks,
                min_snapshots_to_keep=retention.min_snapshots_to_keep,
                context=context,
            )

        @dg.job(name=f"tradinghub_eu_{endpoint.lower()}_maintenance_job")
        def _maintenance_job():
            _expire_snapshots_op()

        return _maintenance_job
