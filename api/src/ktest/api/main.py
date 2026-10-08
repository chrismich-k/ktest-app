"""ktest API: one FastAPI app for all frontend data, served under /api."""

from fastapi import APIRouter, FastAPI

from ktest.api import balancing_costs

app = FastAPI(
    title="ktest API",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)

api = APIRouter(prefix="/api")


@api.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


api.include_router(balancing_costs.router)
app.include_router(api)
