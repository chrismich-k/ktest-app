"""ENTSOG datasets this code location can import: table schema and parser.

Records are taken as delivered, one row per record, with the API's field
names in snake case; empty strings become null. Fields the API adds later
are ignored until they are added here.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pyarrow as pa


def _string(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _integer(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _boolean(value: Any) -> bool | None:
    """Booleans come as true/false, 0/1 or "0"/"1"."""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value.lower() in ("1", "true")
    return bool(value)


def _timestamp(value: Any) -> datetime | None:
    """ISO 8601 with offset."""
    if value is None or value == "":
        return None
    return datetime.fromisoformat(value)


@dataclass(frozen=True)
class Field:
    api_name: str
    column: str
    type: pa.DataType
    parse: Callable[[Any], Any]


def _schema(leading: list[pa.Field], fields: list[Field]) -> pa.Schema:
    return pa.schema(
        [
            *leading,
            *(pa.field(f.column, f.type) for f in fields),
            pa.field("fetched_at", pa.timestamp("us", tz="UTC"), False),
        ]
    )


def _parse(fields: list[Field], record: dict[str, Any]) -> dict[str, Any]:
    return {f.column: f.parse(record.get(f.api_name)) for f in fields}


def _string_field(api_name: str, column: str) -> Field:
    return Field(api_name, column, pa.string(), _string)


def _boolean_field(api_name: str, column: str) -> Field:
    return Field(api_name, column, pa.bool_(), _boolean)


def _timestamp_field(api_name: str, column: str) -> Field:
    return Field(api_name, column, pa.timestamp("us", tz="UTC"), _timestamp)


# --- operational data (endpoint operationaldatas) ---------------------------

# the same fields for all indicators (physical flow, nomination, allocation,
# capacities, interruptions); fields of other indicators stay null
_OPERATIONAL_FIELDS = [
    _string_field("indicator", "indicator"),
    _string_field("periodType", "period_type"),
    # start and end of the period in the operator's local time; for daily
    # values the operator's gas day, which does not start at 06:00 for all
    # operators
    _timestamp_field("periodFrom", "period_from"),
    _timestamp_field("periodTo", "period_to"),
    _string_field("operatorKey", "operator_key"),
    _string_field("tsoEicCode", "tso_eic_code"),
    _string_field("operatorLabel", "operator_label"),
    _string_field("pointKey", "point_key"),
    _string_field("pointLabel", "point_label"),
    _string_field("tsoItemIdentifier", "tso_item_identifier"),
    # entry / exit
    _string_field("directionKey", "direction_key"),
    _string_field("unit", "unit"),
    Field("value", "value", pa.float64(), _number),
    # Provisional / Confirmed
    _string_field("flowStatus", "flow_status"),
    # placeholder records of points without data for the indicator (value
    # null), repeated for every day; the period they apply from is
    # original_period_from
    _boolean_field("isNA", "is_na"),
    _timestamp_field("originalPeriodFrom", "original_period_from"),
    _boolean_field("isUnlimited", "is_unlimited"),
    _string_field("interruptionType", "interruption_type"),
    _string_field("restorationInformation", "restoration_information"),
    _string_field(
        "interruptionCalculationRemark", "interruption_calculation_remark"
    ),
    _string_field("capacityType", "capacity_type"),
    _string_field("capacityBookingStatus", "capacity_booking_status"),
    _boolean_field("isCamRelevant", "is_cam_relevant"),
    _boolean_field("isCmpRelevant", "is_cmp_relevant"),
    _string_field("bookingPlatformKey", "booking_platform_key"),
    _string_field("bookingPlatformLabel", "booking_platform_label"),
    _string_field("bookingPlatformURL", "booking_platform_url"),
    _string_field("pointType", "point_type"),
    Field("idPointType", "id_point_type", pa.int64(), _integer),
    _boolean_field("isArchived", "is_archived"),
    _string_field("itemRemarks", "item_remarks"),
    _string_field("generalRemarks", "general_remarks"),
    _timestamp_field("lastUpdateDateTime", "last_update_at"),
    # the API's record id
    _string_field("id", "record_id"),
]

OPERATIONAL_SCHEMA = _schema(
    # the requested day; the records of a daily query are the operators'
    # gas days starting on that date
    [pa.field("gas_day", pa.date32(), nullable=False)],
    _OPERATIONAL_FIELDS,
)


def parse_operational_day(
    records: list[dict[str, Any]], gas_day: date
) -> list[dict[str, Any]]:
    rows = []
    for record in records:
        if record["periodFrom"][:10] != gas_day.isoformat():
            # placeholder records come twice at times (at the start of
            # their validity and on DST change days), the second one for
            # the next day; the next day's query returns it as well
            if record.get("isNA"):
                continue
            raise ValueError(
                f"requested gas day {gas_day}, got period starting"
                f" {record['periodFrom']}"
            )
        rows.append(
            {"gas_day": gas_day, **_parse(_OPERATIONAL_FIELDS, record)}
        )
    return rows


# --- snapshots of master data -----------------------------------------------


@dataclass(frozen=True)
class Snapshot:
    schema: pa.Schema
    fields: list[Field]


def parse_snapshot(
    snapshot: Snapshot, records: list[dict[str, Any]], snapshot_date: date
) -> list[dict[str, Any]]:
    return [
        {"snapshot_date": snapshot_date, **_parse(snapshot.fields, record)}
        for record in records
    ]


def _side_fields(side: str) -> list[Field]:
    """The fields of one side ("from" / "to") of an interconnection."""
    s = side
    return [
        _string_field(f"{s}SystemLabel", f"{s}_system_label"),
        # Transmission, Distribution, Storage, LNG Terminals, Production,
        # Final Consumers
        _string_field(
            f"{s}InfrastructureTypeLabel", f"{s}_infrastructure_type_label"
        ),
        _string_field(f"{s}CountryKey", f"{s}_country_key"),
        _string_field(f"{s}CountryLabel", f"{s}_country_label"),
        # balancing zone
        _string_field(f"{s}BzKey", f"{s}_bz_key"),
        _string_field(f"{s}BzLabel", f"{s}_bz_label"),
        _string_field(f"{s}BzLabelLong", f"{s}_bz_label_long"),
        _string_field(f"{s}OperatorKey", f"{s}_operator_key"),
        _string_field(f"{s}OperatorLabel", f"{s}_operator_label"),
        _string_field(f"{s}OperatorLongLabel", f"{s}_operator_long_label"),
        _string_field(f"{s}PointKey", f"{s}_point_key"),
        _string_field(f"{s}PointLabel", f"{s}_point_label"),
        _string_field(f"{s}TsoItemIdentifier", f"{s}_tso_item_identifier"),
        _string_field(f"{s}TsoPointLabel", f"{s}_tso_point_label"),
        _string_field(f"{s}DirectionKey", f"{s}_direction_key"),
        _boolean_field(f"{s}HasData", f"{s}_has_data"),
        _boolean_field(f"{s}IsCAM", f"{s}_is_cam"),
        _boolean_field(f"{s}IsCMP", f"{s}_is_cmp"),
        _string_field(f"{s}BookingPlatformKey", f"{s}_booking_platform_key"),
        _string_field(
            f"{s}BookingPlatformLabel", f"{s}_booking_platform_label"
        ),
        _string_field(f"{s}BookingPlatformURL", f"{s}_booking_platform_url"),
    ]


# one record per interconnection between two systems at a point; a flow
# record (operator_key, point_key, direction_key) matches the from side
# (exit) or the to side (entry)
_INTERCONNECTION_FIELDS = [
    # the API's record id, unique per snapshot
    _string_field("id", "record_id"),
    _string_field("pointKey", "point_key"),
    _string_field("pointLabel", "point_label"),
    _boolean_field("isSingleOperator", "is_single_operator"),
    # position on the platform's schematic map, not geographic
    Field("pointTpMapX", "point_tp_map_x", pa.float64(), _number),
    Field("pointTpMapY", "point_tp_map_y", pa.float64(), _number),
    *_side_fields("from"),
    *_side_fields("to"),
    _string_field("entryTpNeMoUsage", "entry_tp_ne_mo_usage"),
    _string_field("exitTpNeMoUsage", "exit_tp_ne_mo_usage"),
    _string_field("validFrom", "valid_from"),
    _string_field("validto", "valid_to"),
    _boolean_field("isInvalid", "is_invalid"),
    # as delivered, e.g. "Nov  6 2026  6:47PM"
    _string_field("lastUpdateDateTime", "last_update_date_time"),
]

_SNAPSHOT_DATE = pa.field("snapshot_date", pa.date32(), nullable=False)

# by endpoint
SNAPSHOTS: dict[str, Snapshot] = {
    "interconnections": Snapshot(
        schema=_schema([_SNAPSHOT_DATE], _INTERCONNECTION_FIELDS),
        fields=_INTERCONNECTION_FIELDS,
    ),
}
