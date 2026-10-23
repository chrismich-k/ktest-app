import dagster as dg

from ktest.mastr.asset_factory import build_raw_archive_asset
from ktest.mastr.asset_keys import KATALOGWERTE
from ktest.mastr.xml_rows import FieldSpec

_FIELD_MAP = [
    FieldSpec("Id", "id", "int", required=True),
    FieldSpec("Wert", "wert", "string", required=True),
    # Raw, unresolved FK into Katalogkategorien.Id - deliberately NOT
    # joined here (raw-data-untouched-derive-in-dbt).
    FieldSpec(
        "KatalogKategorieId", "katalog_kategorie_id", "int", required=True
    ),
]


@dg.definitions
def defs() -> dg.Definitions:
    return build_raw_archive_asset(
        key=KATALOGWERTE,
        table_name="Katalogwerte",
        row_tag="Katalogwert",
        file_prefix="Katalogwerte",
        field_map=_FIELD_MAP,
        description=(
            "MaStR catalog values (Katalogwerte.xml) - the lookup rows"
            " referenced by coded fields across every other MaStR object"
            " type. KatalogKategorieId kept as a raw int FK, not resolved."
            " Natural key: Id (confirmed globally unique across all"
            " categories, not just within one KatalogKategorieId), unique"
            " only within a single stichtag - unique across the whole"
            " table only as (Id, stichtag)."
        ),
    )
