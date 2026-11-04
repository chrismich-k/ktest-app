"""GIE datasets this code location can import: platform, table schema and
parser per dataset.

A request for one gas day without further filters returns the whole
platform as a tree: region (EU / Non-EU) > country > company > facility.
Only the facilities are imported, one row per facility and gas day, with
the codes and names of their parents; aggregates belong in dbt. Values are
taken as delivered, except that the API's "-" for "no value" becomes null.
"""

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pyarrow as pa

# depth of the facility nodes in the tree (region = 0)
FACILITY_DEPTH = 3


@dataclass(frozen=True)
class Dataset:
    host: str
    schema: pa.Schema
    parse_facility: Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class ParsedDay:
    rows: list[dict[str, Any]]
    # facility nodes without a code (seen on AGSI for some early days, all
    # values zero); they have no key and are skipped
    skipped_without_code: int


def _number(value: Any) -> float | None:
    if value is None or value == "-":
        return None
    return float(value)


def _optional_string(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def parse_day(
    dataset: Dataset,
    data: list[dict[str, Any]],
    gas_day: date,
    countries: list[str] | None,
) -> ParsedDay:
    """The facility rows of one gas day's tree, optionally only those of
    the given countries."""
    rows: list[dict[str, Any]] = []
    skipped = 0

    def walk(
        node: dict[str, Any], path: list[dict[str, Any]]
    ) -> Iterator[tuple[list[dict[str, Any]], dict[str, Any]]]:
        if len(path) == FACILITY_DEPTH:
            yield path, node
            return
        for child in node.get("children") or []:
            yield from walk(child, [*path, node])

    for region in data:
        for (_, country, company), facility in walk(region, []):
            if countries is not None and country["code"] not in countries:
                continue
            if "code" not in facility:
                skipped += 1
                continue
            if facility["gasDayStart"] != gas_day.isoformat():
                raise ValueError(
                    f"requested gas day {gas_day}, got"
                    f" {facility['gasDayStart']}"
                )
            rows.append(
                {
                    "gas_day": gas_day,
                    "gas_day_end": date.fromisoformat(facility["gasDayEnd"]),
                    "region": region["code"],
                    "country_code": country["code"],
                    "country_name": country["name"],
                    "company_eic": company["code"],
                    "company_name": company["name"],
                    "facility_eic": facility["code"],
                    "facility_name": facility["name"],
                    **dataset.parse_facility(facility),
                    "status": facility["status"],
                    "info": json.dumps(facility["info"])
                    if facility.get("info")
                    else None,
                    "updated_at": datetime.fromisoformat(
                        facility["updatedAt"]
                    ),
                }
            )
    return ParsedDay(rows=rows, skipped_without_code=skipped)


def _key_fields() -> list[pa.Field]:
    return [
        pa.field("gas_day", pa.date32(), nullable=False),
        pa.field("gas_day_end", pa.date32(), nullable=False),
        # eu / ne (Non-EU)
        pa.field("region", pa.string(), nullable=False),
        # ISO code; GB* marks the post-Brexit view of the GB facilities,
        # which carry the same EICs
        pa.field("country_code", pa.string(), nullable=False),
        pa.field("country_name", pa.string(), nullable=False),
        pa.field("company_eic", pa.string(), nullable=False),
        pa.field("company_name", pa.string(), nullable=False),
        pa.field("facility_eic", pa.string(), nullable=False),
        pa.field("facility_name", pa.string(), nullable=False),
    ]


def _trailing_fields() -> list[pa.Field]:
    return [
        # E (estimated), C (confirmed), N (no data)
        pa.field("status", pa.string(), nullable=False),
        # service announcements as delivered (JSON array), null if none
        pa.field("info", pa.string()),
        # "last touched" timestamp as delivered, time zone not documented
        pa.field("updated_at", pa.timestamp("us"), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]


# --- AGSI: gas storage facilities ------------------------------------------

_AGSI_SCHEMA = pa.schema(
    [
        *_key_fields(),
        # DSR depleted field, ASR aquifer, ASF salt cavern, SRC rock cavern,
        # GRP virtual storage group
        pa.field("facility_type", pa.string()),
        pa.field("longitude", pa.float64()),
        pa.field("latitude", pa.float64()),
        # TWh, end of gas day
        pa.field("gas_in_storage", pa.float64()),
        # GWh/d
        pa.field("injection", pa.float64()),
        pa.field("withdrawal", pa.float64()),
        pa.field("net_withdrawal", pa.float64()),
        # TWh, technical capacity
        pa.field("working_gas_volume", pa.float64()),
        # GWh/d
        pa.field("injection_capacity", pa.float64()),
        pa.field("withdrawal_capacity", pa.float64()),
        # TWh
        pa.field("contracted_capacity", pa.float64()),
        pa.field("available_capacity", pa.float64()),
        # gas in storage / working gas volume, %
        pa.field("full", pa.float64()),
        # (injection - withdrawal) / working gas volume
        pa.field("trend", pa.float64()),
        *_trailing_fields(),
    ]
)


def _parse_agsi_facility(facility: dict[str, Any]) -> dict[str, Any]:
    return {
        "facility_type": _optional_string(facility.get("type")),
        "longitude": _number(facility.get("longitude")),
        "latitude": _number(facility.get("latitude")),
        "gas_in_storage": _number(facility.get("gasInStorage")),
        "injection": _number(facility.get("injection")),
        "withdrawal": _number(facility.get("withdrawal")),
        "net_withdrawal": _number(facility.get("netWithdrawal")),
        "working_gas_volume": _number(facility.get("workingGasVolume")),
        "injection_capacity": _number(facility.get("injectionCapacity")),
        "withdrawal_capacity": _number(facility.get("withdrawalCapacity")),
        "contracted_capacity": _number(facility.get("contractedCapacity")),
        "available_capacity": _number(facility.get("availableCapacity")),
        "full": _number(facility.get("full")),
        "trend": _number(facility.get("trend")),
    }


DATASETS: dict[str, Dataset] = {
    "agsi": Dataset(
        host="agsi.gie.eu",
        schema=_AGSI_SCHEMA,
        parse_facility=_parse_agsi_facility,
    ),
}
