"""XSD-driven schema discovery and XML row parsing for Trading Hub Europe's
DataExport API, which is documented here:
``https://api.tradinghub.eu/api/dataexport/manual/en``

THE publishes an XSD for every endpoint at ``GET /schema/{Endpunkt}``,
generated directly from its live data structures (manual chapter 8). Rather
than hand-writing a pyarrow schema per endpoint, this module reads that XSD
at asset execution time and builds the schema generically - the same code
path works for all ~50 endpoints.

Verified against THE's real, currently-published schemas/responses (not just
the manual's prose) for ``VTPVolumesMonthly`` (a ``dateOnly``-typed field via
the imported ``microsoft-com-wsdl-types`` namespace) and
``AggregateImbalancePositions`` (a plain ``xs:dateTime`` field, and - contrary
to the manual's parameter table implying a single "current" snapshot - a
response that in practice already spans more than one gas day). Both confirm
the shape documented in chapter 8: the XML root and each repeated row element
share the same tag name, and a "date" field's *type* (not its name) is what
identifies it - hence the generic, name-agnostic detection below rather than
a hardcoded field-name mapping.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import dagster as dg
import httpx
import pyarrow as pa
from lxml import etree

from ktest.tradinghub_eu.http import get as http_get

XS_NS = "http://www.w3.org/2001/XMLSchema"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"

_XS_TYPE_TO_ARROW = {
    "decimal": pa.float64(),
    "double": pa.float64(),
    "float": pa.float64(),
    "int": pa.int64(),
    "integer": pa.int64(),
    "long": pa.int64(),
    "short": pa.int64(),
    "boolean": pa.bool_(),
}


@dataclass
class XsdFieldInfo:
    name: str
    is_date: bool
    pa_type: pa.DataType


@dataclass
class TheEndpointSchema:
    """The XSD-derived shape of one THE endpoint's response rows.

    ``root_element_name`` is the XML root/row tag as declared by the XSD's
    own root ``xs:element`` - THE's own docs promise this matches the
    endpoint's URL path segment ("wrapper and repeated row share the same
    tag name"), but in practice it does not always match the *endpoint*
    name itself (confirmed against the live API: ``FeesAndLevies``'s XSD
    declares a root element named ``FeesAndCharges``). Row detection in
    :func:`rows_to_arrow_table` therefore compares each child's tag against
    the *document's own root tag* at parse time, not against this field or
    the endpoint name - self-referential and correct regardless of which
    name THE actually used.
    """

    endpoint: str
    root_element_name: str
    fields: list[XsdFieldInfo]
    date_field: str | None
    """Name of the first date/dateTime-typed field, if any - used to derive
    each row's synthesized partition column generically. ``None`` for
    endpoints whose rows genuinely carry no date-typed field at all."""


_SCHEMA_CACHE: dict[str, TheEndpointSchema] = {}


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag.split(":")[-1]


def _find_complex_type(root: etree._Element, name: str) -> etree._Element:
    for complex_type in root.findall(f"{{{XS_NS}}}complexType"):
        if complex_type.get("name") == name:
            return complex_type
    raise ValueError(
        f"complexType {name!r} not found in schema for its endpoint"
    )


def _resolve_is_date(
    type_attr: str, elem: etree._Element, import_root: etree._Element | None
) -> bool:
    prefix, sep, local_name = type_attr.partition(":")
    if not sep:
        local_name, prefix = prefix, None

    namespace_uri = elem.nsmap.get(prefix) if prefix else None
    if prefix is None or namespace_uri == XS_NS:
        return local_name in ("date", "dateTime")

    if import_root is None:
        return False
    for simple_type in import_root.iter(f"{{{XS_NS}}}simpleType"):
        if simple_type.get("name") == local_name:
            restriction = simple_type.find(f"{{{XS_NS}}}restriction")
            if restriction is not None:
                base = restriction.get("base", "").split(":")[-1]
                return base in ("date", "dateTime")
    return False


def _pa_type_for(type_attr: str, is_date: bool) -> pa.DataType:
    if is_date:
        return pa.timestamp("us")
    local_name = type_attr.split(":")[-1]
    return _XS_TYPE_TO_ARROW.get(local_name, pa.string())


def fetch_xsd_schema(
    endpoint: str,
    user_agent: str,
    context: dg.AssetExecutionContext | dg.OpExecutionContext,
) -> TheEndpointSchema:
    """``GET /schema/{endpoint}`` (and its ``xs:import``, if any) and derive
    a :class:`TheEndpointSchema`. Memoized per-process/per-endpoint - safe
    since this only ever runs inside an already-executing op/asset, never at
    component/definitions load time.
    """
    if endpoint in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[endpoint]

    with httpx.Client() as client:
        main_response = http_get(
            client,
            f"/schema/{endpoint}",
            params=None,
            user_agent=user_agent,
            context=context,
        )
        main_root = etree.fromstring(main_response.content)

        import_root = None
        import_elem = main_root.find(f"{{{XS_NS}}}import")
        if import_elem is not None:
            schema_location = import_elem.get("schemaLocation")
            import_response = http_get(
                client,
                f"/schema/{schema_location}",
                params=None,
                user_agent=user_agent,
                context=context,
            )
            import_root = etree.fromstring(import_response.content)

    root_elem_decl = main_root.find(f"{{{XS_NS}}}element")
    root_complex_type = _find_complex_type(
        main_root, root_elem_decl.get("type")
    )
    row_elem_decl = root_complex_type.find(f".//{{{XS_NS}}}element")
    row_complex_type = _find_complex_type(main_root, row_elem_decl.get("type"))

    fields: list[XsdFieldInfo] = []
    date_field: str | None = None
    for field_elem in row_complex_type.findall(
        f"{{{XS_NS}}}sequence/{{{XS_NS}}}element"
    ):
        name = field_elem.get("name")
        type_attr = field_elem.get("type", "xs:string")
        is_date = _resolve_is_date(type_attr, field_elem, import_root)
        fields.append(
            XsdFieldInfo(
                name=name,
                is_date=is_date,
                pa_type=_pa_type_for(type_attr, is_date),
            )
        )
        if is_date and date_field is None:
            date_field = name

    schema = TheEndpointSchema(
        endpoint=endpoint,
        root_element_name=root_elem_decl.get("name"),
        fields=fields,
        date_field=date_field,
    )
    _SCHEMA_CACHE[endpoint] = schema
    return schema


def _cast(raw: str | None, field: XsdFieldInfo):
    if raw is None:
        return None
    if field.is_date:
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return datetime.fromisoformat(f"{raw}T00:00:00")
    if pa.types.is_floating(field.pa_type):
        return float(raw)
    if pa.types.is_integer(field.pa_type):
        return int(raw)
    if pa.types.is_boolean(field.pa_type):
        return raw.strip().lower() == "true"
    return raw


def _partition_value_from(
    raw_date: str | None, fallback: date | None
) -> date | None:
    if not raw_date:
        return fallback
    return date.fromisoformat(raw_date[:10])


def rows_to_arrow_table(
    xml_bytes: bytes,
    schema: TheEndpointSchema,
    *,
    sort_columns: list[str],
    fallback_gas_day: date | None,
    partition_column: str,
) -> pa.Table:
    """Parse one THE XML response into a sorted ``pa.Table``, tagged with a
    synthesized partition column (named ``partition_column`` - e.g.
    ``dg_daily_partition``/``dg_monthly_partition``, matching whatever this
    asset's Dagster ``partitions_def`` granularity is; the column's *role* is
    "which Dagster/Iceberg partition this row belongs to", not a business
    date field in its own right - THE's own date-typed field, whatever it's
    actually called, is kept verbatim as its own separate column below).
    Row elements are found generically (children of root whose tag equals
    the endpoint name - see chapter 8's wrapper-equals-row-tag quirk); row
    fields are read generically from each row's own children, so a
    newly-added element THE starts returning shows up as a new column
    automatically instead of being dropped or erroring (THE's own
    backward-compatibility policy, manual chapter 13, guarantees only
    additive changes without notice).
    """
    root = etree.fromstring(xml_bytes)
    # Self-referential on purpose: THE's own docs promise the root and each
    # repeated row share the same tag, but that tag doesn't always match the
    # *endpoint* name (confirmed live: FeesAndLevies's root is
    # <FeesAndCharges>) - compare against the document's own root tag, not
    # `schema.endpoint`/`schema.root_element_name`, so this holds regardless.
    row_elems = [child for child in root if child.tag == root.tag]

    field_by_name = {f.name: f for f in schema.fields}
    columns: dict[str, list] = {f.name: [] for f in schema.fields}
    extra_columns: dict[str, list] = {}
    partition_values: list[date | None] = []

    for i, row in enumerate(row_elems):
        row_values: dict[str, str | None] = {}
        for child in row:
            name = _local_name(child.tag)
            is_nil = child.get(f"{{{XSI_NS}}}nil") == "true"
            row_values[name] = None if is_nil else child.text

        for f in schema.fields:
            columns[f.name].append(_cast(row_values.get(f.name), f))

        for name, raw in row_values.items():
            if name in field_by_name:
                continue
            if name not in extra_columns:
                extra_columns[name] = [None] * i
            extra_columns[name].append(raw)
        for name in extra_columns:
            if len(extra_columns[name]) <= i:
                extra_columns[name].append(None)

        raw_date = (
            row_values.get(schema.date_field) if schema.date_field else None
        )
        partition_values.append(
            _partition_value_from(raw_date, fallback_gas_day)
        )

    arrays: dict[str, pa.Array] = {}
    for f in schema.fields:
        arrays[f.name] = pa.array(columns[f.name], type=f.pa_type)
    for name, values in extra_columns.items():
        arrays[name] = pa.array(values, type=pa.string())
    arrays[partition_column] = pa.array(partition_values, type=pa.date32())

    table = pa.table(arrays)
    if table.num_rows:
        table = table.sort_by([(c, "ascending") for c in sort_columns])
    return table
