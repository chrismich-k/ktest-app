import dagster as dg

# Partition keys are discovered at runtime (see defs/sensors.py), not
# computable in advance - mirrors mastr's own dynamic stichtag partitions,
# even though BKG's VG250 cadence is yearly rather than daily/quarterly.
bkg_stichtag_partitions_def = dg.DynamicPartitionsDefinition(
    name="bkg_stichtag"
)
