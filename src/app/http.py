from __future__ import annotations

import secrets
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

from .config import AppConfig
from .databricks import DatabricksClientError
from .finops_service import FinOpsService, FinOpsServiceError


class SyncRequest(BaseModel):
    runs_limit: int = 250


def create_app(config: AppConfig, base_dir: Optional[Path] = None) -> FastAPI:
    root_dir = base_dir or Path.cwd()
    asset_dir = root_dir / "frontend"
    service = FinOpsService(config)
    basic_auth = HTTPBasic(auto_error=False)

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

    def require_auth(
        credentials: Optional[HTTPBasicCredentials] = Depends(basic_auth),
    ) -> None:
        if not config.basic_auth_enabled:
            return

        if credentials is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required.",
                headers={"WWW-Authenticate": "Basic"},
            )

        username_ok = secrets.compare_digest(
            credentials.username, config.basic_auth_user or ""
        )
        password_ok = secrets.compare_digest(
            credentials.password, config.basic_auth_password or ""
        )
        if not username_ok or not password_ok:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials.",
                headers={"WWW-Authenticate": "Basic"},
            )

    def enforce_write_origin(request: Request) -> None:
        origin = request.headers.get("origin")
        if not origin:
            return

        normalized_origin = origin.rstrip("/")
        if config.allowed_origins:
            if normalized_origin in config.allowed_origins:
                return
            raise FinOpsServiceError(f"Origin '{normalized_origin}' is not allowed.")

        host = request.headers.get("host")
        if not host:
            return
        if urlsplit(normalized_origin).netloc == host:
            return
        raise FinOpsServiceError(
            "Cross-origin writes are blocked by default. Configure FACEBRICK_ALLOWED_ORIGINS if you need them."
        )

    @app.get("/", include_in_schema=False, dependencies=[Depends(require_auth)])
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
        dependencies=[Depends(require_auth)],
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

    @app.get("/api/health", dependencies=[Depends(require_auth)])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/finops/sync", dependencies=[Depends(require_auth)])
    async def sync(payload: SyncRequest, request: Request) -> object:
        enforce_write_origin(request)
        return service.sync(runs_limit=payload.runs_limit)

    @app.get("/api/finops/dashboard", dependencies=[Depends(require_auth)])
    async def dashboard(window_days: int = 30) -> object:
        return service.get_dashboard(window_days=window_days)

    @app.get("/api/finops/summary", dependencies=[Depends(require_auth)])
    async def summary(window_days: int = 30) -> object:
        return service.get_summary(window_days=window_days)

    @app.get("/api/finops/jobs", dependencies=[Depends(require_auth)])
    async def jobs(window_days: int = 30) -> object:
        return service.get_jobs(window_days=window_days)

    @app.get("/api/finops/runs", dependencies=[Depends(require_auth)])
    async def runs(window_days: int = 30, limit: int = 20) -> object:
        return service.get_runs(window_days=window_days, limit=limit)

    @app.get("/api/finops/insights", dependencies=[Depends(require_auth)])
    async def insights(window_days: int = 30) -> object:
        return service.get_insights(window_days=window_days)

    return app


def run_server(config: AppConfig, base_dir: Optional[Path] = None) -> None:
    import uvicorn

    print(
        f"Facebrick listening on http://{config.bind_host}:{config.port} "
        f"(db={config.redacted_database_url}, pricing={config.pricing_file})"
    )
    uvicorn.run(
        create_app(config=config, base_dir=base_dir),
        host=config.bind_host,
        port=config.port,
    )
