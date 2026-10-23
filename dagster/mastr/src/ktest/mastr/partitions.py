import dagster as dg

# Partition keys are discovered at runtime (see defs/sensors.py), not
# computable in advance - the raw-data bucket's set of available stichtag
# snapshots (quarterly Stichtag exports plus daily exports) isn't a fixed
# schedule.
mastr_stichtag_partitions_def = dg.DynamicPartitionsDefinition(
    name="mastr_stichtag"
)
