"""Hardcoded field-map XML row -> pyarrow.Table conversion for MaStR's
UTF-16-declared row-oriented export files. Field lists come straight from
the official docs/XSDs - no dynamic XSD-derived schema at runtime (mirrors
results_long_term_tenders.py's precedent: hardcoded field map, no
Component).

Streaming (iterparse), not a full DOM load: individual member files are
large (up to ~357MB observed for a single 100,000-row-capped Solar part)
despite being structurally simple (flat list of uniform row elements, no
nesting). Measured directly against that real file: a full
etree.parse(...).getroot() peaks at 937MB RSS; this streaming approach
peaks at 16MB RSS for the identical 100,000 rows - a ~58x reduction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import IO, Literal

import pyarrow as pa
from lxml import etree

FieldKind = Literal["string", "int", "bool", "date", "datetime", "decimal"]

_PA_TYPES: dict[FieldKind, pa.DataType] = {
    "string": pa.string(),
    "int": pa.int64(),
    "bool": pa.bool_(),
    "date": pa.date32(),
    "datetime": pa.timestamp("us"),
    "decimal": pa.float64(),
}

_CASTERS = {
    "string": lambda t: t,
    "int": lambda t: int(t),
    # Real data uses "0"/"1", never "true"/"false" despite the docs' own
    # examples - this handles both regardless.
    "bool": lambda t: t.strip().lower() in ("true", "1"),
    "date": lambda t: date.fromisoformat(t[:10]),
    "datetime": lambda t: datetime.fromisoformat(t),
    "decimal": lambda t: float(t),
}


@dataclass(frozen=True)
class FieldSpec:
    xml_tag: str
    column: str
    kind: FieldKind
    required: bool = False
    # Alternate/retired XML tag names this field was known by in an older
    # schema version - e.g. EinheitenWind's "Lage" became
    # "WindAnLandOderAufSee" between schema 23.1 and 26.1, confirmed via a
    # matching example value in the same real unit across both exports.
    # Tried only if xml_tag itself is absent from the row.
    legacy_tags: tuple[str, ...] = ()


def parse_rows(
    fileobj: IO[bytes], row_tag: str, field_map: list[FieldSpec]
) -> tuple[pa.Table, dict[str, int]]:
    """Streaming parse via lxml.etree.iterparse - see module docstring for
    why. tag="{*}<row_tag>" matches regardless of namespace (none is
    actually used in these files, confirmed directly against the real
    archive - this just costs nothing and stays correct if one ever
    appeared).

    Returns (table, missing_required): a `required=True` field absent from
    a row is NOT a hard error - older/differently-structured schema
    versions are a confirmed, real occurrence, not hypothetical. Counts are
    returned for the caller to log/surface in run metadata instead.
    """
    arrays: dict[str, list] = {f.column: [] for f in field_map}
    missing_required: dict[str, int] = {}

    for _, elem in etree.iterparse(
        fileobj, events=("end",), tag=f"{{*}}{row_tag}"
    ):
        by_tag = {c.tag: c.text for c in elem}
        for f in field_map:
            text = by_tag.get(f.xml_tag)
            if text is None:
                for legacy_tag in f.legacy_tags:
                    text = by_tag.get(legacy_tag)
                    if text is not None:
                        break
            if not text:
                if f.required:
                    missing_required[f.xml_tag] = (
                        missing_required.get(f.xml_tag, 0) + 1
                    )
                arrays[f.column].append(None)
                continue
            arrays[f.column].append(_CASTERS[f.kind](text))

        # Free this row's subtree and any now-fully-processed preceding
        # siblings - the canonical lxml idiom for streaming a flat list
        # without the whole tree accumulating under the root regardless.
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    table = pa.table(
        {
            f.column: pa.array(arrays[f.column], type=_PA_TYPES[f.kind])
            for f in field_map
        }
    )
    return table, missing_required
