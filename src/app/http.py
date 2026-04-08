from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from src.app.config import AppConfig
from src.app.databricks import DatabricksClientError
from src.app.finops_service import FinOpsService, FinOpsServiceError


class SyncRequest(BaseModel):
    runs_limit: int = 250


class ClusterPricingEntryRequest(BaseModel):
    node_type_id: str
    dbus_per_hour: float = Field(ge=0)
    plan: str


class ClusterPricingConfigRequest(BaseModel):
    entries: list[ClusterPricingEntryRequest]


def create_app(config: AppConfig, base_dir: Optional[Path] = None) -> FastAPI:
    root_dir = base_dir or Path.cwd()
    asset_dir = root_dir / "frontend"

    def load_config() -> AppConfig:
        return AppConfig.from_env(base_dir=root_dir)

    def build_service() -> FinOpsService:
        return FinOpsService(load_config())

    app = FastAPI(docs_url=None, redoc_url=None)

    @app.exception_handler(FinOpsServiceError)
    async def handle_finops_error(_: Request, exc: FinOpsServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST, content={"error": str(exc)}
        )

    @app.exception_handler(DatabricksClientError)
    async def handle_databricks_error(
        _: Request, exc: DatabricksClientError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST, content={"error": str(exc)}
        )

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        asset_path = asset_dir / "index.html"
        if not asset_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Missing frontend asset."
            )
        return FileResponse(asset_path)

    @app.get(
        "/assets/{asset_name}",
        include_in_schema=False,
    )
    async def asset(asset_name: str) -> FileResponse:
        if asset_name not in {"app.js", "styles.css"}:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Unknown asset."
            )
        asset_path = asset_dir / asset_name
        if not asset_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Missing frontend asset."
            )
        return FileResponse(asset_path)

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/finops/sync")
    async def sync(payload: SyncRequest) -> object:
        return build_service().sync(runs_limit=payload.runs_limit)

    @app.get("/api/finops/dashboard")
    async def dashboard(window_days: int = 30) -> object:
        return build_service().get_dashboard(window_days=window_days)

    @app.get("/api/finops/summary")
    async def summary(window_days: int = 30) -> object:
        return build_service().get_summary(window_days=window_days)

    @app.get("/api/finops/jobs")
    async def jobs(window_days: int = 30) -> object:
        return build_service().get_jobs(window_days=window_days)

    @app.get("/api/finops/pipelines")
    async def pipelines(window_days: int = 30) -> object:
        return build_service().get_pipelines(window_days=window_days)

    @app.get("/api/finops/tables")
    async def tables(window_days: int = 30) -> object:
        return build_service().get_tables(window_days=window_days)

    @app.get("/api/finops/runs")
    async def runs(window_days: int = 30, limit: int = 20) -> object:
        return build_service().get_runs(window_days=window_days, limit=limit)

    @app.get("/api/finops/insights")
    async def insights(window_days: int = 30) -> object:
        return build_service().get_insights(window_days=window_days)

    @app.get("/api/finops/config")
    async def cluster_pricing_config(refresh: bool = False) -> object:
        return build_service().get_cluster_pricing_config(refresh=refresh)

    @app.post("/api/finops/config")
    async def save_cluster_pricing_config(payload: ClusterPricingConfigRequest) -> object:
        return build_service().save_cluster_pricing_config(
            [item.model_dump() for item in payload.entries]
        )

    return app


def run_server(config: AppConfig, base_dir: Optional[Path] = None) -> None:
    import uvicorn

    print(
        f"Facebrick listening on http://{config.bind_host}:{config.port} "
        f"(db={config.redacted_database_url}, config={config.config_file or 'env-only'})"
    )
    uvicorn.run(
        create_app(config=config, base_dir=base_dir),
        host=config.bind_host,
        port=config.port,
    )
