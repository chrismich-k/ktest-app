"""Balancing costs of THE (external balancing gas + LTO capacity prices).

Reads the dbt view api_balancing_cost_hourly (one row per delivery hour,
source, direction, product and location; amount_eur > 0 cost, < 0
revenue) and aggregates it for the cost chart:

- /timeseries: stacked bars per bucket (stack_side:stack_label) + net
- /breakdown: two-level pie for one bar element (stack_label) of one
  bucket: inner ring direction, outer ring product x location
"""

import time
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ktest.api import db

router = APIRouter(prefix="/balancing-costs", tags=["balancing costs"])

TABLE = db.dataware_table("api_balancing_cost_hourly")

Grain = Literal["hour", "day", "week", "month"]
DayBasis = Literal["gas_day", "calendar_date"]

# hourly bars: ~1500 at most, more is unreadable and slow in an SVG chart
MAX_HOURLY_DAYS = 62


class Filters(BaseModel):
    start: date
    end: date
    day_basis: DayBasis
    include_lto: bool
    gas_quality: list[str]
    location_type: list[str]

    def where(self) -> tuple[str, list[Any]]:
        """WHERE clause (with ? placeholders) and its parameters."""
        # day_basis is a Literal, safe to put into the SQL
        clauses = [f"{self.day_basis} between ? and ?"]
        params: list[Any] = [self.start, self.end]
        if not self.include_lto:
            clauses.append("source <> 'LTO'")
        for column, values in (
            ("gas_quality", self.gas_quality),
            ("location_type", self.location_type),
        ):
            if values:
                placeholders = ", ".join("?" * len(values))
                clauses.append(f"{column} in ({placeholders})")
                params.extend(values)
        return " and ".join(clauses), params


def _filters(
    start: Annotated[date, Query(description="First day (inclusive)")],
    end: Annotated[date, Query(description="Last day (inclusive)")],
    day_basis: DayBasis,
    include_lto: bool,
    gas_quality: Annotated[list[str] | None, Query()],
    location_type: Annotated[list[str] | None, Query()],
) -> Filters:
    if end < start:
        raise HTTPException(422, "end is before start")
    return Filters(
        start=start,
        end=end,
        day_basis=day_basis,
        include_lto=include_lto,
        gas_quality=gas_quality or [],
        location_type=location_type or [],
    )


def _bucket_expression(grain: Grain, day_basis: DayBasis) -> str:
    if grain == "hour":
        return "hour_start_utc"
    if grain == "day":
        return day_basis
    return f"date_trunc('{grain}', {day_basis})"


def _bucket_key(value: date | datetime) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return value.isoformat()


class Series(BaseModel):
    key: str
    stack_label: str
    stack_side: Literal["buy", "sell"]


class Bucket(BaseModel):
    bucket: str
    values: dict[str, float]
    net: float


class Timeseries(BaseModel):
    grain: Grain
    series: list[Series]
    buckets: list[Bucket]


@router.get("/timeseries", response_model=Timeseries)
def timeseries(
    grain: Grain,
    start: Annotated[date, Query(description="First day (inclusive)")],
    end: Annotated[date, Query(description="Last day (inclusive)")],
    day_basis: DayBasis = "gas_day",
    include_lto: bool = True,
    gas_quality: Annotated[list[str] | None, Query()] = None,
    location_type: Annotated[list[str] | None, Query()] = None,
) -> Timeseries:
    """Signed amounts per bucket and stack series, plus the net amount."""
    filters = _filters(
        start, end, day_basis, include_lto, gas_quality, location_type
    )
    if grain == "hour" and (end - start).days >= MAX_HOURLY_DAYS:
        raise HTTPException(
            422, f"hourly resolution is limited to {MAX_HOURLY_DAYS} days"
        )
    where, params = filters.where()
    rows = db.query(
        f"""
        select
            {_bucket_expression(grain, day_basis)} as bucket,
            stack_side,
            stack_label,
            sum(amount_eur) as amount_eur
        from {TABLE}
        where {where}
        group by 1, 2, 3
        order by 1
        """,
        params,
    )

    series: dict[str, Series] = {}
    buckets: dict[str, dict[str, float]] = defaultdict(dict)
    for row in rows:
        key = f"{row['stack_side']}:{row['stack_label']}"
        series.setdefault(
            key,
            Series(
                key=key,
                stack_label=row["stack_label"],
                stack_side=row["stack_side"],
            ),
        )
        buckets[_bucket_key(row["bucket"])][key] = row["amount_eur"]

    return Timeseries(
        grain=grain,
        series=[series[key] for key in sorted(series)],
        buckets=[
            Bucket(bucket=bucket, values=values, net=sum(values.values()))
            for bucket, values in buckets.items()
        ],
    )


def _bucket_range(
    grain: Grain, bucket: str
) -> tuple[date | datetime, date | datetime]:
    """[first, after last) of a bucket key returned by /timeseries."""
    try:
        if grain == "hour":
            hour = datetime.fromisoformat(bucket)
            if hour.tzinfo is None:
                raise ValueError("hour bucket needs a time zone")
            return hour, hour + timedelta(hours=1)
        day = date.fromisoformat(bucket)
    except ValueError as error:
        raise HTTPException(422, f"invalid bucket: {error}") from error
    if grain == "day":
        return day, day + timedelta(days=1)
    if grain == "week":
        return day, day + timedelta(days=7)
    next_month = (day.replace(day=28) + timedelta(days=4)).replace(day=1)
    return day, next_month


class Slice(BaseModel):
    direction: str
    product_label: str | None = None
    location_label: str | None = None
    amount_eur: float


class Breakdown(BaseModel):
    inner: list[Slice]
    outer: list[Slice]


@router.get("/breakdown", response_model=Breakdown)
def breakdown(
    grain: Grain,
    bucket: Annotated[str, Query(description="Bucket key from /timeseries")],
    stack_label: str,
    start: Annotated[date, Query(description="First day (inclusive)")],
    end: Annotated[date, Query(description="Last day (inclusive)")],
    day_basis: DayBasis = "gas_day",
    include_lto: bool = True,
    gas_quality: Annotated[list[str] | None, Query()] = None,
    location_type: Annotated[list[str] | None, Query()] = None,
) -> Breakdown:
    """Pie data for one bar element: both directions of stack_label.

    Amounts are absolute values (pie slices). start/end/filters are the
    ones of the chart, so the pie matches a partially covered bucket.
    """
    filters = _filters(
        start, end, day_basis, include_lto, gas_quality, location_type
    )
    where, params = filters.where()
    first, after = _bucket_range(grain, bucket)
    column = "hour_start_utc" if grain == "hour" else day_basis
    if grain == "hour":
        bucket_clause = (
            f"{column} >= from_iso8601_timestamp(?)"
            f" and {column} < from_iso8601_timestamp(?)"
        )
        params += [first.isoformat(), after.isoformat()]
    else:
        bucket_clause = f"{column} >= ? and {column} < ?"
        params += [first, after]
    rows = db.query(
        f"""
        select
            direction,
            product_label,
            location_label,
            abs(sum(amount_eur)) as amount_eur
        from {TABLE}
        where {where} and {bucket_clause} and stack_label = ?
        group by 1, 2, 3
        having sum(amount_eur) <> 0
        order by 1, 4 desc
        """,
        [*params, stack_label],
    )
    outer = [Slice(**row) for row in rows]
    inner_amounts: dict[str, float] = defaultdict(float)
    for piece in outer:
        inner_amounts[piece.direction] += piece.amount_eur
    return Breakdown(
        # same order as outer (sorted by direction), so the rings line up
        inner=[
            Slice(direction=direction, amount_eur=amount)
            for direction, amount in inner_amounts.items()
        ],
        outer=outer,
    )


class StackLabel(BaseModel):
    stack_label: str
    source: str


class Meta(BaseModel):
    stack_labels: list[StackLabel]
    gas_qualities: list[str]
    location_types: list[str]
    first_gas_day: date
    # LTO option periods reach into the future; the balancing actions end
    # at the latest imported gas day (the sensible default chart end)
    last_gas_day: date
    last_balancing_gas_day: date


_META_TTL_S = 600
_meta_cache: tuple[float, Meta] | None = None


@router.get("/meta", response_model=Meta)
def meta() -> Meta:
    """Filter options and all stack labels (for stable chart colours)."""
    global _meta_cache
    if _meta_cache and time.monotonic() - _meta_cache[0] < _META_TTL_S:
        return _meta_cache[1]
    rows = db.query(
        f"""
        select stack_label, source, gas_quality, location_type,
            min(gas_day) as first_gas_day, max(gas_day) as last_gas_day
        from {TABLE}
        group by 1, 2, 3, 4
        """,
        [],
    )
    result = Meta(
        stack_labels=[
            StackLabel(source=source, stack_label=label)
            for source, label in sorted(
                {(r["source"], r["stack_label"]) for r in rows}
            )
        ],
        gas_qualities=sorted({r["gas_quality"] for r in rows}),
        location_types=sorted(
            {r["location_type"] for r in rows if r["location_type"]}
        ),
        first_gas_day=min(r["first_gas_day"] for r in rows),
        last_gas_day=max(r["last_gas_day"] for r in rows),
        last_balancing_gas_day=max(
            r["last_gas_day"] for r in rows if r["source"] == "EBG"
        ),
    )
    _meta_cache = (time.monotonic(), result)
    return result
