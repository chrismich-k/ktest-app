"""ENTSO-E document types this code location can import: query
parameters, table schema and parser per document type.

Each parser decodes the document into one row per delivered point and keeps
the series attributes as columns; nothing is derived beyond the timestamp
(period start + (position - 1) * resolution). Points omitted by curve type
A03 (value unchanged) are not filled in here, that belongs in dbt.
"""

import re
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pyarrow as pa

TIMESTAMP = pa.timestamp("us", tz="UTC")


@dataclass(frozen=True)
class Dataset:
    schema: pa.Schema
    # column holding the zone EIC, part of the overwrite filter
    zone_column: str
    query_params: Callable[[str], dict[str, str]]
    parse: Callable[[ET.Element], Iterator[dict[str, Any]]]


def _resolution(value: str) -> timedelta:
    match = re.fullmatch(r"PT(\d+)([MH])", value)
    if match is None:
        raise ValueError(f"unsupported resolution {value!r}")
    amount = int(match.group(1))
    return (
        timedelta(minutes=amount)
        if match.group(2) == "M"
        else timedelta(hours=amount)
    )


class _Document:
    """Namespace-agnostic access to a market document."""

    def __init__(self, root: ET.Element):
        self.root = root
        self.ns = root.tag[: root.tag.index("}") + 1]

    def find(self, element: ET.Element, path: str) -> ET.Element | None:
        return element.find("/".join(self.ns + p for p in path.split("/")))

    def text(self, element: ET.Element, path: str) -> str:
        found = self.find(element, path)
        if found is None or found.text is None:
            raise ValueError(f"missing element {path!r}")
        return found.text.strip()

    def optional_text(self, element: ET.Element, path: str) -> str | None:
        found = self.find(element, path)
        if found is None or found.text is None:
            return None
        return found.text.strip()

    def findall(self, element: ET.Element, name: str) -> list[ET.Element]:
        return element.findall(self.ns + name)

    def header(self) -> dict[str, Any]:
        return {
            "document_mrid": self.text(self.root, "mRID"),
            "revision_number": int(self.text(self.root, "revisionNumber")),
            "document_created_at": datetime.fromisoformat(
                self.text(self.root, "createdDateTime")
            ),
        }

    def points(
        self, series: ET.Element
    ) -> Iterator[tuple[datetime, str, int, ET.Element]]:
        """(timestamp, resolution, position, point) of every point."""
        for period in self.findall(series, "Period"):
            start = datetime.fromisoformat(
                self.text(period, "timeInterval/start")
            )
            resolution = self.text(period, "resolution")
            step = _resolution(resolution)
            for point in self.findall(period, "Point"):
                position = int(self.text(point, "position"))
                yield (
                    start + (position - 1) * step,
                    resolution,
                    position,
                    point,
                )


# --- A44: day-ahead prices (Publication_MarketDocument) ---------------------

_DAY_AHEAD_PRICES_SCHEMA = pa.schema(
    [
        pa.field("ts", TIMESTAMP, nullable=False),
        pa.field("resolution", pa.string(), nullable=False),
        pa.field("position", pa.int32(), nullable=False),
        pa.field("bidding_zone", pa.string(), nullable=False),
        pa.field("contract_type", pa.string(), nullable=False),
        pa.field("auction_type", pa.string()),
        pa.field("business_type", pa.string()),
        # classificationSequence_AttributeInstanceComponent.position: tells
        # several price series of the same zone apart (DE-LU and AT have
        # two, the coupled SDAC price is 1); absent for most zones
        pa.field("sequence", pa.int32()),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("price_unit", pa.string(), nullable=False),
        pa.field("curve_type", pa.string()),
        pa.field("document_mrid", pa.string(), nullable=False),
        pa.field("revision_number", pa.int32(), nullable=False),
        pa.field("document_created_at", TIMESTAMP, nullable=False),
        pa.field("fetched_at", TIMESTAMP, nullable=False),
    ]
)


def _day_ahead_prices_params(eic: str) -> dict[str, str]:
    return {"documentType": "A44", "in_Domain": eic, "out_Domain": eic}


def _parse_day_ahead_prices(root: ET.Element) -> Iterator[dict[str, Any]]:
    doc = _Document(root)
    header = doc.header()
    for series in doc.findall(root, "TimeSeries"):
        sequence = doc.optional_text(
            series,
            "classificationSequence_AttributeInstanceComponent.position",
        )
        attributes = {
            "bidding_zone": doc.text(series, "in_Domain.mRID"),
            "contract_type": doc.text(series, "contract_MarketAgreement.type"),
            "auction_type": doc.optional_text(series, "auction.type"),
            "business_type": doc.optional_text(series, "businessType"),
            "sequence": None if sequence is None else int(sequence),
            "currency": doc.text(series, "currency_Unit.name"),
            "price_unit": doc.text(series, "price_Measure_Unit.name"),
            "curve_type": doc.optional_text(series, "curveType"),
            **header,
        }
        for ts, resolution, position, point in doc.points(series):
            yield {
                "ts": ts,
                "resolution": resolution,
                "position": position,
                "price": float(doc.text(point, "price.amount")),
                **attributes,
            }


DATASETS: dict[str, Dataset] = {
    "A44": Dataset(
        schema=_DAY_AHEAD_PRICES_SCHEMA,
        zone_column="bidding_zone",
        query_params=_day_ahead_prices_params,
        parse=_parse_day_ahead_prices,
    ),
}
