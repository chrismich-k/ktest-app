import dagster as dg

from ktest.mastr._common_einheit_fields import COMMON_EINHEIT_FIELDS
from ktest.mastr.asset_factory import build_raw_archive_asset
from ktest.mastr.asset_keys import EINHEITEN_WIND
from ktest.mastr.xml_rows import FieldSpec

_WIND_FIELDS = COMMON_EINHEIT_FIELDS + [
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
    FieldSpec("NameWindpark", "name_windpark", "string"),
    # Confirmed schema-23.1-era rename (matching example value 888 for the
    # same real unit across both exports) - see plan's "Confirmed real
    # schema drift" section.
    FieldSpec(
        "WindAnLandOderAufSee",
        "wind_an_land_oder_auf_see",
        "int",
        required=True,
        legacy_tags=("Lage",),
    ),
    FieldSpec("Seelage", "seelage", "int"),
    # Confirmed schema-23.1-era renames (terminology change: informal
    # "Cluster" naming -> the formal "Flächenentwicklungsplan" term).
    FieldSpec(
        "GebietNachDemFlaechenentwicklungsplanOstsee",
        "gebiet_nach_dem_flaechenentwicklungsplan_ostsee",
        "int",
        legacy_tags=("ClusterOstsee",),
    ),
    FieldSpec(
        "GebietNachDemFlaechenentwicklungsplanNordsee",
        "gebiet_nach_dem_flaechenentwicklungsplan_nordsee",
        "int",
        legacy_tags=("ClusterNordsee",),
    ),
    FieldSpec("Hersteller", "hersteller", "int"),
    FieldSpec("Technologie", "technologie", "int"),
    FieldSpec("Typenbezeichnung", "typenbezeichnung", "string"),
    FieldSpec("Nabenhoehe", "nabenhoehe", "decimal"),
    FieldSpec("Rotordurchmesser", "rotordurchmesser", "decimal"),
    FieldSpec(
        "Rotorblattenteisungssystem", "rotorblattenteisungssystem", "bool"
    ),
    FieldSpec(
        "AuflageAbschaltungLeistungsbegrenzung",
        "auflage_abschaltung_leistungsbegrenzung",
        "bool",
    ),
    FieldSpec(
        "AuflagenAbschaltungSchallimmissionsschutzNachts",
        "auflagen_abschaltung_schallimmissionsschutz_nachts",
        "bool",
    ),
    FieldSpec(
        "AuflagenAbschaltungSchallimmissionsschutzTagsueber",
        "auflagen_abschaltung_schallimmissionsschutz_tagsueber",
        "bool",
    ),
    FieldSpec(
        "AuflagenAbschaltungSchattenwurf",
        "auflagen_abschaltung_schattenwurf",
        "bool",
    ),
    FieldSpec(
        "AuflagenAbschaltungTierschutz",
        "auflagen_abschaltung_tierschutz",
        "bool",
    ),
    FieldSpec(
        "AuflagenAbschaltungEiswurf", "auflagen_abschaltung_eiswurf", "bool"
    ),
    FieldSpec(
        "AuflagenAbschaltungSonstige", "auflagen_abschaltung_sonstige", "bool"
    ),
    FieldSpec("Wassertiefe", "wassertiefe", "decimal"),
    FieldSpec("Kuestenentfernung", "kuestenentfernung", "decimal"),
    FieldSpec("Nachtkennzeichnung", "nachtkennzeichnung", "bool"),
    FieldSpec("Buergerenergie", "buergerenergie", "bool"),
    FieldSpec("EegMaStRNummer", "eeg_mastr_nummer", "string", required=True),
    FieldSpec(
        "TechnologieFlugwindenergieanlage",
        "technologie_flugwindenergieanlage",
        "int",
    ),
    FieldSpec("Flughoehe", "flughoehe", "decimal"),
    FieldSpec("Flugradius", "flugradius", "decimal"),
    FieldSpec(
        "InbetriebnahmedatumAmAktuellenStandort",
        "inbetriebnahmedatum_am_aktuellen_standort",
        "date",
    ),
    FieldSpec(
        "GroesseDerInAnspruchGenommenenFlaeche",
        "groesse_der_in_anspruch_genommenen_flaeche",
        "decimal",
    ),
    # int on Wind - confirmed quirk: Solar's own field of the same name is
    # a string instead (see einheiten_solar.py). Not a transcription error.
    FieldSpec(
        "UeberwiegendeNutzungsartDerFlaecheVorErrichtung",
        "ueberwiegende_nutzungsart_der_flaeche_vor_errichtung",
        "int",
    ),
    FieldSpec(
        "VorherigerNutzungsartenbereichDerFlaeche",
        "vorheriger_nutzungsartenbereich_der_flaeche",
        "int",
    ),
]


@dg.definitions
def defs() -> dg.Definitions:
    return build_raw_archive_asset(
        key=EINHEITEN_WIND,
        table_name="EinheitenWind",
        row_tag="EinheitWind",
        file_prefix="EinheitenWind",
        field_map=_WIND_FIELDS,
        description=(
            "MaStR wind power units (EinheitenWind.xml / _N.xml). Natural"
            " key: EinheitMastrNummer, unique only within a single"
            " stichtag - unique across the whole table only as"
            " (EinheitMastrNummer, stichtag)."
        ),
    )
