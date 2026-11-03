"""Client for the ENTSO-E Transparency Platform REST API.

There is a single endpoint; the query parameters (documentType, domains,
periodStart/periodEnd) select the data. Responses are XML documents, some
document types (e.g. balancing) come zipped. "No data" is not an empty
response but an Acknowledgement_MarketDocument with reason code 999.
"""

import io
import logging
import time
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Generator, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import dagster as dg
import httpx

API_URL = "https://web-api.tp.entsoe.eu/api"
# the API allows 400 requests per minute and token; stay well below that
REQUEST_INTERVAL_SECONDS = 0.25
MAX_ATTEMPTS = 5
NO_DATA_REASON_CODE = "999"
# the API accepts up to one year per request; smaller chunks keep the
# responses small
MAX_REQUEST_SPAN = timedelta(days=90)

# httpx logs request URLs, which contain the security token
logging.getLogger("httpx").setLevel(logging.WARNING)


class EntsoeApiError(Exception):
    pass


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_documents(body: bytes) -> list[ET.Element]:
    if body.startswith(b"PK"):
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            return [
                ET.fromstring(zf.read(name))
                for name in sorted(zf.namelist())
                if name.endswith(".xml")
            ]
    return [ET.fromstring(body)]


def _acknowledgement(root: ET.Element) -> tuple[str | None, str]:
    """Reason code and text of an Acknowledgement_MarketDocument."""
    code = text = None
    for element in root.iter():
        name = _local_name(element.tag)
        if name == "code" and code is None:
            code = element.text
        elif name == "text" and text is None:
            text = element.text
    return code, text or ""


def format_period(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y%m%d%H%M")


def request_chunks(
    start: datetime, end: datetime
) -> Iterator[tuple[datetime, datetime]]:
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + MAX_REQUEST_SPAN, end)
        yield chunk_start, chunk_end
        chunk_start = chunk_end


class EntsoeClient:
    def __init__(self, http: httpx.Client, token: str):
        self._http = http
        self._token = token
        self._last_request = 0.0

    def _get(self, params: Mapping[str, str]) -> httpx.Response:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            wait = self._last_request + REQUEST_INTERVAL_SECONDS
            time.sleep(max(0.0, wait - time.monotonic()))
            self._last_request = time.monotonic()
            try:
                response = self._http.get(
                    API_URL, params={"securityToken": self._token, **params}
                )
            except httpx.TransportError as e:
                # never chain the original exception: it may carry the URL
                # and with it the token
                if attempt == MAX_ATTEMPTS:
                    raise EntsoeApiError(
                        f"request failed: {type(e).__name__}"
                    ) from None
            else:
                if response.status_code != 429 and response.status_code < 500:
                    return response
                if attempt == MAX_ATTEMPTS:
                    raise EntsoeApiError(f"HTTP {response.status_code}")
            time.sleep(2**attempt)
        raise AssertionError("unreachable")

    def query(self, params: Mapping[str, str]) -> list[ET.Element]:
        """All market documents for the query; empty if there is no data."""
        response = self._get(params)
        try:
            documents = _parse_documents(response.content)
        except (ET.ParseError, zipfile.BadZipFile):
            raise EntsoeApiError(
                f"HTTP {response.status_code}, response is neither XML nor"
                " a zip archive"
            ) from None
        acknowledgements = [
            d
            for d in documents
            if _local_name(d.tag) == "Acknowledgement_MarketDocument"
        ]
        if acknowledgements:
            code, text = _acknowledgement(acknowledgements[0])
            if code == NO_DATA_REASON_CODE:
                return []
            raise EntsoeApiError(
                f"HTTP {response.status_code}, reason {code}: {text}"
            )
        if response.status_code != 200:
            raise EntsoeApiError(f"HTTP {response.status_code}")
        return documents


class EntsoeApiResource(dg.ConfigurableResource):
    token: str

    @contextmanager
    def client(self) -> Generator[EntsoeClient]:
        with httpx.Client(timeout=120.0) as http:
            yield EntsoeClient(http, self.token)
