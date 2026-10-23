import dagster as dg

from ktest.mastr.asset_factory import build_raw_archive_asset
from ktest.mastr.asset_keys import KATALOGKATEGORIEN
from ktest.mastr.xml_rows import FieldSpec

_FIELD_MAP = [
    FieldSpec("Id", "id", "int", required=True),
    FieldSpec("Name", "name", "string", required=True),
]


@dg.definitions
def defs() -> dg.Definitions:
    return build_raw_archive_asset(
        key=KATALOGKATEGORIEN,
        table_name="Katalogkategorien",
        # Root element is "Katalogkategorien" (plural), row element is
        # "Katalogkategorie" (singular) - confirmed against the real file.
        row_tag="Katalogkategorie",
        file_prefix="Katalogkategorien",
        field_map=_FIELD_MAP,
        description=(
            "MaStR catalog categories (Katalogkategorien.xml) - Id+Name"
            " lookup of each category referenced by Katalogwerte. Natural"
            " key: Id, unique only within a single stichtag (every"
            " stichtag re-exports the full current state) - unique across"
            " the whole table only as (Id, stichtag)."
        ),
    )
