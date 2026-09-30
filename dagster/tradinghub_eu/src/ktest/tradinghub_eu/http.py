"""Shared HTTP helper for Trading Hub Europe's DataExport API.

See https://api.tradinghub.eu/api/dataexport/manual/de for the full manual.
Key rules this module exists to enforce:

- A ``User-Agent`` identifying the calling application is required on every
  request (manual chapter 2).
- ``429``/``503`` responses carry a ``Retry-After`` header (seconds) that
  must be honored before retrying (chapters 5.3/5.4) - THE explicitly asks
  clients to run requests sequentially, never in parallel.
- Errors are ``application/problem+json`` per RFC 9457 (chapter 7); the
  machine-readable ``code`` field is what should be branched on, not the
  free-text ``title``/``detail``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import dagster as dg
import httpx

THE_BASE_URL = "https://api.tradinghub.eu/api/dataexport/xmlexport"

# THE's own query times out server-side after 75s, upstream infra after 90s
# (manual 5.3) - stay comfortably above both.
DEFAULT_TIMEOUT = httpx.Timeout(
    connect=10.0, read=120.0, write=10.0, pool=10.0
)

DEFAULT_RETRY_AFTER_SECONDS = 5.0
MAX_RETRIES = 10


@dataclass
class TheApiProblem:
    """An RFC 9457 application/problem+json error body from THE's API."""

    status: int
    code: str | None
    title: str | None
    detail: str | None
    raw: dict


class TheApiError(Exception):
    def __init__(self, problem: TheApiProblem):
        self.problem = problem
        super().__init__(
            f"THE API error {problem.status} ({problem.code}):{problem.detail}"
        )


def _parse_problem(response: httpx.Response) -> TheApiProblem:
    try:
        body = response.json()
    except ValueError:
        body = {}
    return TheApiProblem(
        status=response.status_code,
        code=body.get("code"),
        title=body.get("title"),
        detail=body.get("detail"),
        raw=body,
    )


def get(
    client: httpx.Client,
    path: str,
    *,
    params: dict[str, str] | None,
    user_agent: str,
    context: dg.AssetExecutionContext | dg.OpExecutionContext,
) -> httpx.Response:
    """GET ``{THE_BASE_URL}{path}`` with THE's mandated User-Agent header,
    retrying on 429/503 per their ``Retry-After`` header. Raises
    ``TheApiError`` for any other non-2xx response.
    """
    headers = {"User-Agent": user_agent}
    url = f"{THE_BASE_URL}{path}"

    for attempt in range(MAX_RETRIES):
        response = client.get(
            url, params=params, headers=headers, timeout=DEFAULT_TIMEOUT
        )

        if response.status_code in (429, 503):
            retry_after = float(
                response.headers.get(
                    "Retry-After", DEFAULT_RETRY_AFTER_SECONDS
                )
            )
            context.log.warning(
                f"THE API returned {response.status_code} for {url} "
                f"(attempt {attempt + 1}/{MAX_RETRIES}); retrying after"
                f"{retry_after}s"
            )
            time.sleep(retry_after)
            continue

        if response.status_code >= 400:
            raise TheApiError(_parse_problem(response))

        return response

    raise TheApiError(
        TheApiProblem(
            status=503,
            code="max-retries-exceeded",
            title="Max retries exceeded",
            detail=f"Gave up after {MAX_RETRIES} attempts against {url}",
            raw={},
        )
    )
