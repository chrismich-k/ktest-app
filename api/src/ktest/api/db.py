"""Trino access, configured from API_* env vars.

All variables are required (no defaults): the deployment decides which
data is read. They are read at import, so a missing one stops the server
at startup instead of failing on the first request.
"""

import os
import re
from contextlib import closing
from typing import Any

import trino
from trino.auth import BasicAuthentication

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _identifier(name: str) -> str:
    value = os.environ[name]
    if not _IDENTIFIER.match(value):
        raise ValueError(f"{name} is not a plain SQL identifier: {value!r}")
    return value


HOST = os.environ["API_TRINO_HOST"]
PORT = int(os.environ["API_TRINO_PORT"])
HTTP_SCHEME = os.environ["API_TRINO_HTTP_SCHEME"]
AUTH_METHOD = os.environ["API_TRINO_AUTH_METHOD"]
USER = os.environ["API_TRINO_USER"]
CATALOG = _identifier("API_TRINO_CATALOG")
DATAWARE_SCHEMA = _identifier("API_DATAWARE_SCHEMA")

if AUTH_METHOD == "ldap":
    _AUTH = BasicAuthentication(USER, os.environ["API_TRINO_PASSWORD"])
elif AUTH_METHOD == "none":
    _AUTH = None
else:
    raise ValueError(
        f"API_TRINO_AUTH_METHOD must be 'ldap' or 'none': {AUTH_METHOD!r}"
    )


def dataware_table(name: str) -> str:
    """Fully qualified name of a dbt api_* view."""
    return f"{CATALOG}.{DATAWARE_SCHEMA}.{name}"


def query(sql: str, params: list[Any]) -> list[dict[str, Any]]:
    """Run a query with ? placeholders and return the rows as dicts."""
    connection = trino.dbapi.connect(
        host=HOST,
        port=PORT,
        http_scheme=HTTP_SCHEME,
        user=USER,
        auth=_AUTH,
        catalog=CATALOG,
        source="ktest-api",
    )
    with closing(connection):
        cursor = connection.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in rows]
