import dagster as dg

from ktest.mastr._common_einheit_fields import COMMON_EINHEIT_FIELDS
from ktest.mastr.asset_factory import build_raw_archive_asset
from ktest.mastr.asset_keys import EINHEITEN_SOLAR
from ktest.mastr.xml_rows import FieldSpec

_SOLAR_FIELDS = COMMON_EINHEIT_FIELDS + [
    FieldSpec(
        "AnschlussAnHoechstOderHochSpannung",
        "anschluss_an_hoechst_oder_hoch_spannung",
        "bool",
    ),
    FieldSpec("Einsatzverantwortlicher", "einsatzverantwortlicher", "string"),
    FieldSpec("FernsteuerbarkeitNb", "fernsteuerbarkeit_nb", "bool"),
    FieldSpec("FernsteuerbarkeitDv", "fernsteuerbarkeit_dv", "bool"),
    FieldSpec("Einspeisungsart", "einspeisungsart", "int"),
    FieldSpec("GenMastrNummer", "gen_mastr_nummer", "string"),
    FieldSpec(
        "ZugeordneteWirkleistungWechselrichter",
        "zugeordnete_wirkleistung_wechselrichter",
        "decimal",
    ),
    FieldSpec("AnzahlModule", "anzahl_module", "int"),
    # Confirmed schema-23.1-era rename (matching example value 853, same
    # position between AnzahlModule/Leistungsbegrenzung, for the same real
    # unit across both exports) - see plan's "Confirmed real schema drift"
    # section. Note this is a *different* rename than Wind's own "Lage".
    FieldSpec(
        "ArtDerSolaranlage",
        "art_der_solaranlage",
        "int",
        required=True,
        legacy_tags=("Lage",),
    ),
    FieldSpec("Leistungsbegrenzung", "leistungsbegrenzung", "int"),
    FieldSpec(
        "EinheitlicheAusrichtungUndNeigungswinkel",
        "einheitliche_ausrichtung_und_neigungswinkel",
        "bool",
    ),
    FieldSpec("Hauptausrichtung", "hauptausrichtung", "int"),
    FieldSpec(
        "HauptausrichtungNeigungswinkel",
        "hauptausrichtung_neigungswinkel",
        "int",
    ),
    FieldSpec("Nebenausrichtung", "nebenausrichtung", "int"),
    FieldSpec(
        "NebenausrichtungNeigungswinkel",
        "nebenausrichtung_neigungswinkel",
        "int",
    ),
    FieldSpec(
        "GroesseDerInAnspruchGenommenenFlaecheInHektar",
        "groesse_der_in_anspruch_genommenen_flaeche_in_hektar",
        "decimal",
    ),
    # string on Solar - int on Wind's own field of the same name. Confirmed
    # quirk per the docs, not a transcription error.
    FieldSpec(
        "UeberwiegendeNutzungsartDerFlaecheVorErrichtung",
        "ueberwiegende_nutzungsart_der_flaeche_vor_errichtung",
        "string",
    ),
    FieldSpec("Nutzungsbereich", "nutzungsbereich", "int"),
    FieldSpec("Buergerenergie", "buergerenergie", "bool"),
    FieldSpec("EegMaStRNummer", "eeg_mastr_nummer", "string", required=True),
    FieldSpec("SpeicherAmGleichenOrt", "speicher_am_gleichen_ort", "bool"),
    FieldSpec(
        "InbetriebnahmedatumAmAktuellenStandort",
        "inbetriebnahmedatum_am_aktuellen_standort",
        "date",
    ),
    FieldSpec("NameDesSolarparks", "name_des_solarparks", "string"),
    # string on Solar - int on Wind. Same confirmed quirk as above.
    FieldSpec(
        "VorherigerNutzungsartenbereichDerFlaeche",
        "vorheriger_nutzungsartenbereich_der_flaeche",
        "string",
    ),
    # Comma-separated multi-value - kept as a single raw string, not split
    # into a list column (raw-data-untouched-derive-in-dbt: splitting is a
    # derivation, left to dbt).
    FieldSpec(
        "ZusaetzlicheMerkmaleDerFlaecheUndDerAktuellenFlaechennutzung",
        "zusaetzliche_merkmale_der_flaeche_und_der_aktuellen_flaechennutzung",
        "string",
    ),
    FieldSpec("LichteHoehe", "lichte_hoehe", "decimal"),
]


@dg.definitions
def defs() -> dg.Definitions:
    return build_raw_archive_asset(
        key=EINHEITEN_SOLAR,
        table_name="EinheitenSolar",
        row_tag="EinheitSolar",
        file_prefix="EinheitenSolar",
        field_map=_SOLAR_FIELDS,
        description=(
            "MaStR solar power units (EinheitenSolar.xml / _N.xml)."
            " Natural key: EinheitMastrNummer, unique only within a"
            " single stichtag - unique across the whole table only as"
            " (EinheitMastrNummer, stichtag)."
        ),
    )
