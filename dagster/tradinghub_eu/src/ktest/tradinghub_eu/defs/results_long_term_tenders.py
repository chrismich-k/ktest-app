"""Results of Trading Hub Europe's long-term balancing-gas tenders
("Ergebnisse langfristiger Ausschreibungen" - Long Term Options,
Flexibility Services, Short Call Balancing Services, etc.).

Unlike every other asset in this code location, this does NOT come from
the documented DataExport XML API (https://api.tradinghub.eu/api/dataexport/
manual/de) - that API has no endpoint for this data. Instead it's the
undocumented JSON endpoint that powers the table widget on
https://www.tradinghub.eu/de-de/Veroeffentlichungen/Regelenergie/
Ergebnisse-langfristiger-Ausschreibungen - found via the page's own "CSV
download" network request, not published/versioned anywhere. No XSD, no
RFC 9457 problem+json errors, no stability guarantee - THE could change or
remove this at any time without notice. Kept as a standalone asset (not a
TradinghubEuMarketDataComponent instance) since that component is built
specifically around the documented API's XML+XSD shape.

Confirmed live (2026-10-01):
- Earliest data starts 2021-10-01 (gas-month aligned, 06:00 German time).
- The `DatumStart`/`DatumEnde` params filter by *overlap* with the
  requested range, not by exact containment - a request for January also
  returns a December-starting row whose own end date reaches into January
  06:00 (gas-day-aligned periods always spill 6h into the next calendar
  day). Rather than trust that overlap-based query scoping directly (which
  would duplicate every row across its two straddled partitions), each
  row is assigned to exactly one partition by its own `start` month - see
  the asset body for why.
- The response contains genuine duplicate rows (identical in every field)
  within a single query - confirmed up to 74x for the same row. Almost
  certainly one row per individually-awarded lot rather than an aggregated
  count, with no distinguishing field of its own. Not deduplicated here -
  passed through as-is, since collapsing them would silently discard real
  volume information with no way to tell real dupes from synthesized ones.
"""

import calendar
import os
import time
from datetime import date, datetime

import dagster as dg
import httpx
import pyarrow as pa
import pyarrow.compute as pc

LTO_URL = (
    "https://api.tradinghub.eu/api/website/evoq/GetLangfristigeOptionenTabelle"
)
DEFAULT_RETRY_AFTER_SECONDS = 5.0
MAX_RETRIES = 10

# JSON field name -> (Iceberg column name, pyarrow type). `kapazitätstyp` is
# ASCII-sanitized for the Iceberg column name; every other field is already
# ASCII. Order here is the written column order.
_FIELD_MAP: list[tuple[str, str, pa.DataType]] = [
    ("produkt", "produkt", pa.string()),
    ("gasart", "gasart", pa.string()),
    ("ort", "ort", pa.string()),
    ("los", "los", pa.int64()),
    ("leistungspreis", "leistungspreis", pa.float64()),
    ("arbeitspreis", "arbeitspreis", pa.float64()),
    ("flussrichtung", "flussrichtung", pa.string()),
    ("start", "start", pa.timestamp("us")),
    ("ende", "ende", pa.timestamp("us")),
    ("gesamtKosten", "gesamt_kosten", pa.float64()),
    ("vorlaufzeit", "vorlaufzeit", pa.float64()),
    ("recoveryZeit", "recovery_zeit", pa.float64()),
    ("anzahlAbruftage", "anzahl_abruftage", pa.int64()),
    ("berechnungsart", "berechnungsart", pa.string()),
    ("kapazitätstyp", "kapazitaetstyp", pa.string()),
]

PARTITION_COLUMN = "dg_monthly_partition"

partitions_def = dg.MonthlyPartitionsDefinition(
    start_date="2021-10-01",
    # Unlike most data in this code location, the *current* (just-started)
    # month's tenders already exist on day 1 - this is forward-looking
    # contracted-capacity data, not something that only exists once the
    # month has elapsed. Matches the main component's daily default=1
    # reasoning (see TradinghubEuMarketDataComponent.
    # dagster_partition_end_offset's docstring).
    #
    # end_offset=7, not 1: these are tender results for options exercisable
    # in the future, not backward-looking data - THE can (and does) publish
    # results for months well ahead of "now" (the product's relevant date is
    # when the option becomes usable, not when the tender result was
    # published). Per Dagster's own end_offset semantics, N extends the
    # valid partition set N periods beyond the default elapsed-only cutoff;
    # end_offset=7 keeps the current month plus 6 further future months
    # valid/requestable at all times.
    end_offset=7,
)


def _fetch_rows(
    client: httpx.Client,
    datum_start: str,
    datum_ende: str,
    context: dg.AssetExecutionContext,
) -> list[dict]:
    headers = {"User-Agent": os.environ["THE_USER_AGENT"]}
    params = {
        "DatumStart": datum_start,
        "DatumEnde": datum_ende,
        "Subtype_ID": "",
    }
    for attempt in range(MAX_RETRIES):
        response = client.get(
            LTO_URL, params=params, headers=headers, timeout=60.0
        )
        if response.status_code in (429, 503):
            retry_after = float(
                response.headers.get(
                    "Retry-After", DEFAULT_RETRY_AFTER_SECONDS
                )
            )
            context.log.warning(
                f"LTO endpoint returned {response.status_code} "
                f"(attempt {attempt + 1}/{MAX_RETRIES}); retrying after "
                f"{retry_after}s"
            )
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        return response.json()

    raise RuntimeError(
        f"Gave up after {MAX_RETRIES} attempts against {LTO_URL}"
    )


@dg.asset(
    key=dg.AssetKey(["tradinghub_eu", "ResultsLongTermTenders"]),
    description=(
        "Trading Hub Europe - results of long-term balancing-gas tenders"
        " (undocumented endpoint, see module docstring)"
    ),
    group_name="tradinghub_eu",
    kinds={"iceberg"},
    partitions_def=partitions_def,
    metadata={
        "partition_expr": PARTITION_COLUMN,
        "table_properties": {},
        "write_mode": "overwrite",
    },
    pool="tradinghub_eu_api_requests",
)
def results_long_term_tenders(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    month_start = date.fromisoformat(partition_key)
    last_day = calendar.monthrange(month_start.year, month_start.month)[1]
    month_end = month_start.replace(day=last_day)

    with httpx.Client() as client:
        rows = _fetch_rows(
            client,
            month_start.strftime("%m-%d-%Y"),
            month_end.strftime("%m-%d-%Y"),
            context,
        )

    arrays = {}
    for json_key, column_name, pa_type in _FIELD_MAP:
        values = [row.get(json_key) for row in rows]
        if pa_type == pa.timestamp("us"):
            values = [
                datetime.fromisoformat(v) if v is not None else None
                for v in values
            ]
        arrays[column_name] = pa.array(values, type=pa_type)

    # `start`/`ende` are gas-day-aligned (06:00 German time), so a tender
    # whose logical period is "September" has ende=October 1st 06:00 -
    # querying by DatumStart/DatumEnde overlap (not containment) means the
    # API returns that row for *both* September's and October's request.
    # Assign each row to exactly one partition by its own `start` month
    # instead of trusting the query range, so every row lands in exactly
    # one partition regardless of how far `ende` reaches into the next
    # period - handles both this 6h gas-day rollover and genuinely
    # multi-month results the same way, with no arbitrary overlap
    # threshold.
    row_partitions = [
        datetime.fromisoformat(row["start"]).date().replace(day=1)
        for row in rows
    ]
    arrays[PARTITION_COLUMN] = pa.array(row_partitions, type=pa.date32())

    fetched_rows = len(rows)
    table = pa.table(arrays).filter(
        pc.field(PARTITION_COLUMN) == pa.scalar(month_start, type=pa.date32())
    )
    table = table.sort_by(
        [
            ("start", "ascending"),
            ("produkt", "ascending"),
            ("ort", "ascending"),
        ]
    )

    return dg.MaterializeResult(
        value=table,
        metadata={
            "partition_range": f"{month_start}..{month_end}",
            "row_count": table.num_rows,
            "rows_discarded_outside_partition": fetched_rows - table.num_rows,
        },
    )


results_long_term_tenders_refresh_job = dg.define_asset_job(
    name="tradinghub_eu_results_long_term_tenders_refresh_job",
    selection=[results_long_term_tenders.key],
    partitions_def=partitions_def,
)


@dg.schedule(
    name="tradinghub_eu_results_long_term_tenders_refresh_schedule",
    # Daily, not monthly - the *current* month's results can change as new
    # tenders are awarded during the month (same "re-fetch the still-open
    # period" shape as the daily gas-day assets).
    cron_schedule="0 5 * * *",
    job=results_long_term_tenders_refresh_job,
    execution_timezone="Europe/Berlin",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def results_long_term_tenders_refresh_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest:
    now = context.scheduled_execution_time
    partition_key = partitions_def.get_partition_key_for_timestamp(
        now.timestamp()
    )
    return results_long_term_tenders_refresh_job.run_request_for_partition(
        partition_key=partition_key,
        run_key=f"{partition_key}_{now.strftime('%Y%m%dT%H%M%S')}",
        current_time=now,
    )


@dg.op(
    name="tradinghub_eu_results_long_term_tenders_expire_snapshots_op",
    required_resource_keys={"iceberg_maintenance"},
)
def _expire_snapshots_op(context: dg.OpExecutionContext) -> None:
    context.resources.iceberg_maintenance.expire_snapshots(
        table_name="ResultsLongTermTenders",
        max_snapshot_age_weeks=12,
        min_snapshots_to_keep=5,
        context=context,
    )


@dg.job(name="tradinghub_eu_results_long_term_tenders_maintenance_job")
def results_long_term_tenders_maintenance_job():
    _expire_snapshots_op()


results_long_term_tenders_maintenance_schedule = dg.ScheduleDefinition(
    name="tradinghub_eu_results_long_term_tenders_maintenance_schedule",
    job=results_long_term_tenders_maintenance_job,
    cron_schedule="0 3 * * 0",
    execution_timezone="Europe/Berlin",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[results_long_term_tenders],
        jobs=[
            results_long_term_tenders_refresh_job,
            results_long_term_tenders_maintenance_job,
        ],
        schedules=[
            results_long_term_tenders_refresh_schedule,
            results_long_term_tenders_maintenance_schedule,
        ],
    )
