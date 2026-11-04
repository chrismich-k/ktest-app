"""Client for the REST API of the GIE transparency platforms (AGSI for gas
storage, ALSI for LNG terminals).

Both platforms share the API: JSON responses, the key in the `x-key`
header, paging via `page`/`size` with `last_page` in the response. The rate
limit (60 calls per minute and account/IP, every page counts) is shared by
both platforms; exceeding it blocks the account for 60 seconds, repeatedly
exceeding it gets the account banned.
"""

import time
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any

import dagster as dg
import httpx

# stay below 60 calls per minute
REQUEST_INTERVAL_SECONDS = 1.1
# a 429 blocks the account for 60 seconds
RATE_LIMITED_WAIT_SECONDS = 61
MAX_ATTEMPTS = 5
PAGE_SIZE = 300


class GieApiError(Exception):
    pass


@dataclass(frozen=True)
class QueryResult:
    data: list[dict[str, Any]]
    # the platform's latest published gas day; a request for a later day
    # returns this one instead
    latest_gas_day: date


class GieClient:
    def __init__(self, http: httpx.Client):
        self._http = http
        self._last_request = 0.0

    def _get(self, url: str, params: Mapping[str, str]) -> dict[str, Any]:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            wait = self._last_request + REQUEST_INTERVAL_SECONDS
            time.sleep(max(0.0, wait - time.monotonic()))
            self._last_request = time.monotonic()
            try:
                response = self._http.get(url, params=params)
            except httpx.TransportError as e:
                if attempt == MAX_ATTEMPTS:
                    raise GieApiError(
                        f"request failed: {type(e).__name__}"
                    ) from None
                time.sleep(2**attempt)
                continue
            if response.status_code == 200:
                return response.json()
            if attempt == MAX_ATTEMPTS or not (
                response.status_code == 429 or response.status_code >= 500
            ):
                raise GieApiError(f"HTTP {response.status_code}")
            time.sleep(
                RATE_LIMITED_WAIT_SECONDS
                if response.status_code == 429
                else 2**attempt
            )
        raise AssertionError("unreachable")

    def query(self, host: str, params: Mapping[str, str]) -> QueryResult:
        """The `data` entries of all pages."""
        data: list[dict[str, Any]] = []
        page = 1
        while True:
            body = self._get(
                f"https://{host}/api",
                {**params, "page": str(page), "size": str(PAGE_SIZE)},
            )
            data.extend(body["data"])
            if page >= int(body["last_page"]):
                return QueryResult(
                    data=data,
                    latest_gas_day=date.fromisoformat(body["gas_day"]),
                )
            page += 1


class GieApiResource(dg.ConfigurableResource):
    api_key: str

    @contextmanager
    def client(self) -> Generator[GieClient]:
        with httpx.Client(
            headers={"x-key": self.api_key}, timeout=120.0
        ) as http:
            yield GieClient(http)
