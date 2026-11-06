"""Client for the REST API of the ENTSOG Transparency Platform.

Open API without a key, JSON responses; each endpoint returns its records
under its own name (e.g. `operationaldatas`), `limit=-1` returns all
records at once. No rate limit is documented; requests are spaced anyway.

Quirks:
- A query without records is answered with HTTP 404 and the message
  "No result found".
- Large results are cut off silently (seen above about 9,000 records with
  values: a month of physical flows of all points returned only the
  placeholder records for its second half), so data is queried per day.
- Only the last five years are available; older data is archived and
  answered with HTTP 404 and a message pointing to the archives.
"""

import time
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import Any

import dagster as dg
import httpx

BASE_URL = "https://transparency.entsog.eu/api/v1"
REQUEST_INTERVAL_SECONDS = 1.0
MAX_ATTEMPTS = 5
NO_RESULT_MESSAGE = "No result found"


class EntsogApiError(Exception):
    pass


class EntsogClient:
    def __init__(self, http: httpx.Client):
        self._http = http
        self._last_request = 0.0

    def _get(self, url: str, params: Mapping[str, str]) -> httpx.Response:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            wait = self._last_request + REQUEST_INTERVAL_SECONDS
            time.sleep(max(0.0, wait - time.monotonic()))
            self._last_request = time.monotonic()
            try:
                response = self._http.get(url, params=params)
            except httpx.TransportError as e:
                if attempt == MAX_ATTEMPTS:
                    raise EntsogApiError(
                        f"request failed: {type(e).__name__}"
                    ) from None
                time.sleep(2**attempt)
                continue
            if response.status_code in (200, 404):
                return response
            if attempt == MAX_ATTEMPTS or not (
                response.status_code == 429 or response.status_code >= 500
            ):
                raise EntsogApiError(f"HTTP {response.status_code}")
            time.sleep(2**attempt)
        raise AssertionError("unreachable")

    def records(
        self, endpoint: str, params: Mapping[str, str]
    ) -> list[dict[str, Any]]:
        """All records of the endpoint matching the params, empty if there
        are none."""
        response = self._get(
            f"{BASE_URL}/{endpoint}", {**params, "limit": "-1"}
        )
        body = response.json()
        if response.status_code == 404:
            message = body.get("message", "")
            if message == NO_RESULT_MESSAGE:
                return []
            raise EntsogApiError(f"HTTP 404: {message}")
        if endpoint not in body:
            raise EntsogApiError(
                f"response without {endpoint!r} (fields:"
                f" {sorted(body.keys())})"
            )
        return body[endpoint]


class EntsogApiResource(dg.ConfigurableResource):
    @contextmanager
    def client(self) -> Generator[EntsogClient]:
        with httpx.Client(timeout=300.0) as http:
            yield EntsogClient(http)
