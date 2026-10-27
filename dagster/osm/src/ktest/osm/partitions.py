import dagster as dg

# One partition per release tag of yetzt/postleitzahlen (e.g. "2026.02").
postleitzahlen_release_partitions_def = dg.DynamicPartitionsDefinition(
    name="osm_postleitzahlen_release"
)
