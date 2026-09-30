"""Patches a narrow bug in ``dagster-iceberg==0.3.14`` that breaks batched
(multi-partition-per-run) writes.

``dagster_iceberg.handler.IcebergBaseTypeHandler.handle_output``
unconditionally reads ``context.partition_key`` (singular) whenever
``context.has_asset_partitions`` is true, to tag the Iceberg snapshot's commit
properties. ``has_asset_partitions`` is true both for a single-partition run
*and* a multi-partition range run (as produced by
``dg.BackfillPolicy.multi_run(max_partitions_per_run > 1)``), but
``partition_key`` only works for the former - for a range it raises
``check.failed("Tried to access partition_key on a non-partitioned run")``,
because Dagster's own ``OutputContext`` only populates ``_partition_key`` when
``step_context.has_partition_key`` is true (``dagster/_core/execution/context/
output.py``), leaving it ``None`` for a range. Confirmed by reading both
libraries' installed source, and independently corroborated by
https://github.com/dagster-io/dagster/discussions/21515 (the same
``partition_key``-vs-range pitfall is a known, general gotcha for custom IO
managers).

The fix is one line: use ``context.has_partition_key`` (true only for exactly
one partition) instead of ``context.has_asset_partitions`` (true for a range
too). The actual partition-routing/write mechanism does not depend on this line
at all - it is driven by the data's own partition-column values via
``table_slice``/``context.asset_partition_keys`` (plural), which already works
correctly for ranges elsewhere in this same handler. This patch only touches
the one cosmetic ``dagster-partition-key`` commit-metadata tag.

Pinned against ``dagster-iceberg==0.3.14`` exactly (see ``pyproject.toml``).
``apply()`` asserts the pre-patch source still contains the expected buggy
snippet and raises loudly if not, so a future ``dagster-iceberg`` upgrade fails
fast instead of silently leaving a stale patch (or an already-fixed upstream)
in place.
"""

from __future__ import annotations

import inspect

from dagster import MetadataValue, OutputContext, TableColumn, TableSchema
from dagster_iceberg import handler as _handler
from pyiceberg.catalog import Catalog

_EXPECTED_BUGGY_SNIPPET = (
    "context.partition_key if context.has_asset_partitions else None"
)

_applied = False


def _patched_handle_output(
    self, context: OutputContext, table_slice, obj, connection: Catalog
) -> None:
    metadata = context.definition_metadata or {}

    table_properties_usr = metadata.get("table_properties", {})
    partition_spec_update_mode = metadata.get(
        "partition_spec_update_mode", "error"
    )
    schema_update_mode = metadata.get("schema_update_mode", "error")

    partition_field_name_prefix = self._get_partition_field_name_prefix(
        context
    )
    write_mode_with_output_override = self._get_write_mode(context)
    upsert_options = self._get_upsert_options(
        context, write_mode_with_output_override
    )

    _handler.table_writer(
        table_slice=table_slice,
        data=self.to_arrow(obj),
        catalog=connection,
        partition_spec_update_mode=partition_spec_update_mode,
        schema_update_mode=schema_update_mode,
        dagster_run_id=context.run_id,
        # The one-line fix: has_partition_key (single partition only)
        # instead of has_asset_partitions (also true for a multi-partition
        # range, where context.partition_key raises).
        dagster_partition_key=(
            context.partition_key if context.has_partition_key else None
        ),
        table_properties=table_properties_usr,
        write_mode=write_mode_with_output_override,
        partition_field_name_prefix=partition_field_name_prefix,
        upsert_options=upsert_options,
    )

    table_ = connection.load_table(f"{table_slice.schema}.{table_slice.table}")
    current_snapshot = table_.current_snapshot()

    context.add_output_metadata(
        {
            "table_columns": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(name=f["name"], type=str(f["type"]))
                        for f in table_.schema().model_dump()["fields"]
                    ],
                ),
            ),
            **current_snapshot.model_dump(),
        },
    )


def apply() -> None:
    """Idempotently monkeypatch ``dagster_iceberg``'s ``handle_output``. Call
    once at import time from ``defs/resources.py``, before any IOManager is
    used.
    """
    global _applied
    if _applied:
        return

    original_source = inspect.getsource(
        _handler.IcebergBaseTypeHandler.handle_output
    )
    if _EXPECTED_BUGGY_SNIPPET not in original_source:
        raise RuntimeError(
            "dagster_iceberg.handler.IcebergBaseTypeHandler.handle_output no"
            "longer contains the expected buggy snippet - this patch"
            "(ktest.tradinghub_eu.iceberg_io_manager) is pinned to"
            "dagster-iceberg==0.3.14's exact source. Check whether the bug is"
            "fixed upstream (in which case this patch can be removed) before"
            "upgrading dagster-iceberg."
        )

    _handler.IcebergBaseTypeHandler.handle_output = _patched_handle_output
    _applied = True


apply()
