# dataware dbt project

dbt Core project on Trino (Iceberg catalog). Developed and tested standalone
for now; it will later be loaded into the `dataware` Dagster code location
via `dagster-dbt`.

## Setup

dbt-core and dbt-trino are dependencies of the surrounding `dataware` uv
project, so there is no separate virtualenv:

```sh
cd dagster/dataware
uv sync
cp .env.example .env   # fill in DATAWARE_TRINO_* values
```

`profiles.yml` lives in this directory and reads all connection settings
from `DATAWARE_*` env vars. All of them are required (no defaults), so dbt
fails fast instead of silently reading or writing the wrong schema.

dbt-core (>= 1.12) loads `.env` by itself: it searches upwards from the
current directory, so running from here picks up `../.env`. Variables
already set in the shell take precedence over `.env`.

## Usage

Run from `dagster/dataware/dbt`:

```sh
uv run dbt deps     # install packages (dbt_utils)
uv run dbt debug    # check the Trino connection
uv run dbt build    # run models + tests
```

Select the target with `DATAWARE_DBT_TARGET` (`dev` | `prod`) or `--target`.
Override single variables for one run in the shell, e.g.
`DATAWARE_TRADINGHUB_EU_SCHEMA=tradinghub_eu_dev uv run dbt build`.

Commands that don't need a database (`parse`, `ls`) still render the
profile and sources, so they need values too; the placeholders from
`.env.example` are valid for that (e.g. in CI or a Docker build):

```sh
uv run --env-file ../.env.example dbt parse
```

## Schemas

Models land in `DATAWARE_TRINO_SCHEMA_DEV` or `DATAWARE_TRINO_SCHEMA_PROD`,
depending on the target; set only the variable of the target you mean to
write to. Folders with a custom `+schema` get
it appended (dbt default), e.g. staging models go to `dataware_dev_staging`
in dev and `dataware_staging` in prod.

Sources point at the raw Iceberg tables written by the other Dagster code
locations (e.g. `smard`). Their table names start with a digit, hence
`quoting.identifier: true` on the source.
