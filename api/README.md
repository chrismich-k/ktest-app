# ktest API

Single FastAPI service for all data the frontend needs, served under
`/api` (OpenAPI docs at `/api/docs`). It reads the dbt `api_*` views of the
`dataware` project via Trino, not the marts directly, so the serving store
can change later without touching the frontend.

## Setup

```sh
cd api
uv sync
cp .env.example .env   # fill in API_TRINO_* values
```

All `API_*` variables are required (no defaults); the server refuses to
start if one is missing.

## Run locally

```sh
uv run --env-file .env uvicorn ktest.api.main:app --reload --port 8000
```

The Vite dev server (`frontend/`, `npm run dev`) proxies `/api` to
`localhost:8000`.

## Endpoints

- `GET /api/balancing-costs/meta`: filter options, all stack labels (for
  stable chart colours), date range
- `GET /api/balancing-costs/timeseries`: signed costs per bucket
  (`hour|day|week|month` on `gas_day` or `calendar_date`) and stack series
  (`buy|sell:<stack_label>`), plus the net amount
- `GET /api/balancing-costs/breakdown`: two-level pie for one stack label
  in one bucket (inner: direction, outer: product x location)

## Checks

```sh
uv run ruff format && uv run ruff check && uv run ty check
```
