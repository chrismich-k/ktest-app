import dagster as dg

# "Katalogkategorien" (plural) - confirmed against the real extracted
# archive; the PDF's own field table (page 31) says "Katalogkategorie.xml"
# (singular), which is simply wrong. Trust the archive.
KATALOGKATEGORIEN = dg.AssetKey(["mastr", "Katalogkategorien"])
KATALOGWERTE = dg.AssetKey(["mastr", "Katalogwerte"])
EINHEITEN_WIND = dg.AssetKey(["mastr", "EinheitenWind"])
EINHEITEN_SOLAR = dg.AssetKey(["mastr", "EinheitenSolar"])

ALL = [KATALOGKATEGORIEN, KATALOGWERTE, EINHEITEN_WIND, EINHEITEN_SOLAR]
