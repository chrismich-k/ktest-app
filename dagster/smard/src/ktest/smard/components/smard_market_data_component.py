import time
from datetime import datetime, timezone

import dagster as dg
import httpx
import pyarrow as pa

SMARD_BASE_URL = "https://www.smard.de/app/chart_data"
SMARD_CONFIG_URL = (
    "https://www.smard.de/app/chart_configuration/market_data_configuration.json"
)
DAY_MS = 24 * 60 * 60 * 1000
REQUEST_DELAY_SECONDS = 0.1


def _pairs_for(
    client: httpx.Client, category_id: int, sub_id: int, resolution: str
) -> list[tuple[int, str]]:
    """All (data_id, region) pairs for one (category, subcategory, resolution)
    combination, discovered fresh from SMARD's own chart configuration."""
    resp = client.get(SMARD_CONFIG_URL)
    resp.raise_for_status()
    config = resp.json()

    pairs: list[tuple[int, str]] = []
    for category in config["main"]:
        if category["id"] != category_id:
            continue
        for sub in category.get("sub", []):
            if sub["id"] != sub_id:
                continue
            modules = sub.get("module") or {}
            for group in ("default", "other"):
                for mod in modules.get(group) or []:
                    if mod.get("source_resolution") != resolution:
                        continue
                    data_id = mod["data_id"]
                    for region in mod.get("region", []):
                        pairs.append((data_id, region))
    return pairs


class SmardMarketDataComponent(dg.Component, dg.Model, dg.Resolvable):
    """Downloads all SMARD (Bundesnetzagentur Strommarktdaten) time series
    for one (category, subcategory, resolution) combination - across every
    data_id x region pair within it - and writes them into a single Iceberg
    table, partitioned by day.

    SMARD's own chart configuration (market_data_configuration.json) groups
    data_ids by category/subcategory (e.g. category=3 "Markt", sub=31
    "Physikalischer Stromfluss") and declares each data_id's resolution and
    valid regions. A subcategory can contain over a hundred data_ids with
    genuinely different units/semantics across subcategories (prices vs.
    physical flow vs. exchange volume), so each (category_id, sub_id,
    resolution) gets its own table rather than mixing everything together.

    The set of (data_id, region) pairs is discovered fresh on every run,
    filtered to this component's category_id/sub_id/resolution. Requests to
    SMARD are made sequentially with a small delay between pairs, since this
    is an unauthenticated, undocumented public API without published rate
    limits.
    """

    category_id: int
    sub_id: int
    resolution: str
    start_date: str
    spec: dg.ResolvedAssetSpec

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = dg.DailyPartitionsDefinition(start_date=self.start_date)

        @dg.asset(
            key=self.spec.key,
            description=self.spec.description,
            group_name=self.spec.group_name,
            kinds=self.spec.kinds,
            metadata={**self.spec.metadata, "partition_expr": "timestamp"},
            partitions_def=partitions_def,
            # Iceberg commits use optimistic concurrency per table; writing
            # multiple partitions of this asset in parallel causes commit
            # conflicts (CommitFailedException). Serialize writes to it via
            # a pool with a limit of 1 (set separately, see README).
            pool="smard_iceberg_writes",
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            day_start = datetime.strptime(context.partition_key, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            day_start_ms = int(day_start.timestamp() * 1000)
            day_end_ms = day_start_ms + DAY_MS

            timestamps: list[int] = []
            regions: list[str] = []
            data_ids: list[int] = []
            values: list[float | None] = []
            skipped = 0

            with httpx.Client(timeout=30.0) as client:
                pairs = _pairs_for(
                    client, self.category_id, self.sub_id, self.resolution
                )
                context.log.info(f"{len(pairs)} (data_id, region) pairs to fetch")

                for data_id, region in pairs:
                    try:
                        index = client.get(
                            f"{SMARD_BASE_URL}/{data_id}/{region}/"
                            f"index_{self.resolution}.json"
                        )
                        index.raise_for_status()
                        chunk_timestamps = index.json()["timestamps"]
                        candidates = [
                            t for t in chunk_timestamps if t <= day_start_ms
                        ]
                        if not candidates:
                            skipped += 1
                            continue
                        chunk_ts = max(candidates)

                        series_resp = client.get(
                            f"{SMARD_BASE_URL}/{data_id}/{region}/"
                            f"{data_id}_{region}_{self.resolution}_{chunk_ts}.json"
                        )
                        series_resp.raise_for_status()
                        series = series_resp.json()["series"]
                    except httpx.HTTPError as e:
                        context.log.warning(
                            f"Skipping data_id={data_id} region={region}: {e}"
                        )
                        skipped += 1
                        continue

                    for ts, value in series:
                        if day_start_ms <= ts < day_end_ms:
                            timestamps.append(ts * 1000)
                            regions.append(region)
                            data_ids.append(data_id)
                            values.append(value)

                    time.sleep(REQUEST_DELAY_SECONDS)

            context.log.info(
                f"Fetched {len(timestamps)} rows for partition "
                f"{context.partition_key} ({skipped}/{len(pairs)} pairs "
                f"skipped/unavailable)"
            )

            table = pa.Table.from_arrays(
                [
                    pa.array(timestamps, type=pa.timestamp("us")),
                    pa.array(regions, type=pa.string()),
                    pa.array(data_ids, type=pa.int64()),
                    pa.array(values, type=pa.float64()),
                ],
                names=["timestamp", "region", "data_id", "value"],
            )
            return dg.MaterializeResult(
                value=table,
                metadata={
                    "row_count": len(timestamps),
                    "pairs_fetched": len(pairs) - skipped,
                    "pairs_skipped": skipped,
                },
            )

        return dg.Definitions(assets=[_asset])
