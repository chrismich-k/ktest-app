"""Releases of https://github.com/yetzt/postleitzahlen: German postcode
areas from OpenStreetMap (ODbL-1.0), published as a Brotli-compressed
GeoJSON release asset.
"""

import os
from typing import IO

import brotli
import httpx

REPO = "yetzt/postleitzahlen"
ASSET_NAME = "postleitzahlen.geojson.br"
RELEASES_URL = f"https://api.github.com/repos/{REPO}/releases"

_TIMEOUT = httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0)
_CHUNK_SIZE = 1024 * 1024


def _client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": os.environ["OSM_USER_AGENT"]},
        timeout=_TIMEOUT,
        follow_redirects=True,
    )


def list_release_tags() -> list[str]:
    """Tags of all published (non-draft, non-prerelease) releases that
    carry the GeoJSON asset."""
    with _client() as client:
        response = client.get(
            RELEASES_URL,
            params={"per_page": 100},
            headers={"Accept": "application/vnd.github+json"},
        )
        response.raise_for_status()
    return [
        release["tag_name"]
        for release in response.json()
        if not release["draft"]
        and not release["prerelease"]
        and any(asset["name"] == ASSET_NAME for asset in release["assets"])
    ]


def download_geojson(tag: str, dest: IO[bytes]) -> None:
    """Stream the release's GeoJSON asset into `dest`, decompressed (about
    0.5 GB for 24 MB compressed)."""
    url = f"https://github.com/{REPO}/releases/download/{tag}/{ASSET_NAME}"
    decompressor = brotli.Decompressor()
    with _client() as client, client.stream("GET", url) as response:
        response.raise_for_status()
        for chunk in response.iter_bytes(_CHUNK_SIZE):
            dest.write(decompressor.process(chunk))
    if not decompressor.is_finished():
        raise ValueError(f"Truncated Brotli stream from {url}")
