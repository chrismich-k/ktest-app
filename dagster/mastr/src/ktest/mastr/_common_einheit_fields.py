"""Field block shared verbatim between EinheitenWind and EinheitenSolar -
confirmed identical field-for-field in both the official docs and the real
extracted archive."""

from ktest.mastr.xml_rows import FieldSpec

COMMON_EINHEIT_FIELDS: list[FieldSpec] = [
    FieldSpec("EinheitMastrNummer", "einheit_mastr_nummer", "string", True),
    FieldSpec(
        "DatumLetzteAktualisierung",
        "datum_letzte_aktualisierung",
        "datetime",
        True,
    ),
    FieldSpec("LokationMaStRNummer", "lokation_mastr_nummer", "string"),
    FieldSpec(
        "NetzbetreiberpruefungStatus",
        "netzbetreiberpruefung_status",
        "int",
        True,
    ),
    FieldSpec(
        "NetzbetreiberpruefungDatum", "netzbetreiberpruefung_datum", "date"
    ),
    FieldSpec(
        "AnlagenbetreiberMastrNummer",
        "anlagenbetreiber_mastr_nummer",
        "string",
    ),
    FieldSpec("Land", "land", "int", True),
    FieldSpec("Bundesland", "bundesland", "int"),
    FieldSpec("Landkreis", "landkreis", "string"),
    FieldSpec("Gemeinde", "gemeinde", "string"),
    FieldSpec("Gemeindeschluessel", "gemeindeschluessel", "string"),
    FieldSpec("Postleitzahl", "postleitzahl", "string"),
    FieldSpec("Gemarkung", "gemarkung", "string"),
    FieldSpec("FlurFlurstuecknummern", "flur_flurstuecknummern", "string"),
    FieldSpec("Strasse", "strasse", "string"),
    FieldSpec("StrasseNichtGefunden", "strasse_nicht_gefunden", "bool"),
    FieldSpec("Hausnummer", "hausnummer", "string"),
    FieldSpec("Hausnummer_nv", "hausnummer_nv", "bool", True),
    FieldSpec("HausnummerNichtGefunden", "hausnummer_nicht_gefunden", "bool"),
    FieldSpec("Adresszusatz", "adresszusatz", "string"),
    FieldSpec("Ort", "ort", "string"),
    FieldSpec("Laengengrad", "laengengrad", "decimal"),
    FieldSpec("Breitengrad", "breitengrad", "decimal"),
    FieldSpec("Registrierungsdatum", "registrierungsdatum", "date", True),
    FieldSpec(
        "GeplantesInbetriebnahmedatum", "geplantes_inbetriebnahmedatum", "date"
    ),
    FieldSpec("Inbetriebnahmedatum", "inbetriebnahmedatum", "date"),
    FieldSpec(
        "DatumEndgueltigeStilllegung", "datum_endgueltige_stilllegung", "date"
    ),
    FieldSpec(
        "DatumBeginnVoruebergehendeStilllegung",
        "datum_beginn_voruebergehende_stilllegung",
        "date",
    ),
    FieldSpec(
        "DatumWiederaufnahmeBetrieb", "datum_wiederaufnahme_betrieb", "date"
    ),
    FieldSpec("EinheitSystemstatus", "einheit_systemstatus", "int", True),
    FieldSpec("EinheitBetriebsstatus", "einheit_betriebsstatus", "int", True),
    FieldSpec(
        "BestandsanlageMastrNummer", "bestandsanlage_mastr_nummer", "string"
    ),
    FieldSpec(
        "NichtVorhandenInMigriertenEinheiten",
        "nicht_vorhanden_in_migrierten_einheiten",
        "bool",
    ),
    FieldSpec(
        "AltAnlagenbetreiberMastrNummer",
        "alt_anlagenbetreiber_mastr_nummer",
        "string",
    ),
    FieldSpec(
        "DatumDesBetreiberwechsels", "datum_des_betreiberwechsels", "date"
    ),
    FieldSpec(
        "DatumRegistrierungDesBetreiberwechsels",
        "datum_registrierung_des_betreiberwechsels",
        "date",
    ),
    FieldSpec(
        "NameStromerzeugungseinheit",
        "name_stromerzeugungseinheit",
        "string",
        True,
    ),
    FieldSpec("Weic", "weic", "string"),
    FieldSpec("Weic_nv", "weic_nv", "bool", True),
    FieldSpec("WeicDisplayName", "weic_display_name", "string"),
    FieldSpec("Kraftwerksnummer", "kraftwerksnummer", "string"),
    FieldSpec("Kraftwerksnummer_nv", "kraftwerksnummer_nv", "bool", True),
    FieldSpec("Energietraeger", "energietraeger", "int", True),
    FieldSpec("Bruttoleistung", "bruttoleistung", "decimal", True),
    FieldSpec("Nettonennleistung", "nettonennleistung", "decimal", True),
]
