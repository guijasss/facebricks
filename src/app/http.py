from __future__ import annotations

import base64
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Dict, Optional
from urllib.parse import parse_qs, urlparse

from .config import AppConfig
from .databricks import DatabricksClientError
from .finops_service import FinOpsService, FinOpsServiceError


def run_server(config: AppConfig, base_dir: Optional[Path] = None) -> None:
    root_dir = base_dir or Path.cwd()
    service = FinOpsService(config)
    asset_dir = root_dir / "frontend"

    class FacebrickHandler(BaseHTTPRequestHandler):
        server_version = "Facebrick/0.1"

        def do_OPTIONS(self) -> None:
            if not self._authorize():
                return
            self.send_response(HTTPStatus.NO_CONTENT)
            self._write_cors_headers()
            self.end_headers()

        def do_GET(self) -> None:
            self._dispatch("GET")

        def do_POST(self) -> None:
            self._dispatch("POST")

        def _dispatch(self, method: str) -> None:
            if not self._authorize():
                return

            parsed_url = urlparse(self.path)
            if method == "GET" and parsed_url.path in {"/", "/assets/app.js", "/assets/styles.css"}:
                return self._serve_asset(parsed_url.path, asset_dir)

            if method == "GET" and parsed_url.path == "/api/health":
                return self._write_json({"status": "ok"})

            if method == "POST" and parsed_url.path == "/api/finops/sync":
                self._require_safe_origin_for_write()
                try:
                    payload = self._read_json_body()
                    runs_limit = int(payload.get("runs_limit", 250))
                    return self._write_json(service.sync(runs_limit=runs_limit))
                except (ValueError, FinOpsServiceError, DatabricksClientError) as exc:
                    return self._write_error(HTTPStatus.BAD_REQUEST, str(exc))

            if method == "GET" and parsed_url.path == "/api/finops/dashboard":
                return self._handle_finops_get(parsed_url, service.get_dashboard)

            if method == "GET" and parsed_url.path == "/api/finops/summary":
                return self._handle_finops_get(parsed_url, service.get_summary)

            if method == "GET" and parsed_url.path == "/api/finops/jobs":
                return self._handle_finops_get(parsed_url, service.get_jobs)

            if method == "GET" and parsed_url.path == "/api/finops/runs":
                return self._handle_finops_get(parsed_url, service.get_runs, include_limit=True)

            if method == "GET" and parsed_url.path == "/api/finops/insights":
                return self._handle_finops_get(parsed_url, service.get_insights)

            return self._write_error(HTTPStatus.NOT_FOUND, f"No route for {parsed_url.path}")

        def _handle_finops_get(
            self,
            parsed_url: object,
            handler: Callable[..., object],
            include_limit: bool = False,
        ) -> None:
            query = parse_qs(parsed_url.query)
            window_days = int(query.get("window_days", ["30"])[0])
            kwargs = {"window_days": window_days}
            if include_limit:
                kwargs["limit"] = int(query.get("limit", ["20"])[0])
            try:
                self._write_json(handler(**kwargs))
            except FinOpsServiceError as exc:
                self._write_error(HTTPStatus.BAD_REQUEST, str(exc))

        def _serve_asset(self, path: str, asset_dir: Path) -> None:
            asset_map = {
                "/": ("index.html", "text/html; charset=utf-8"),
                "/assets/app.js": ("app.js", "application/javascript; charset=utf-8"),
                "/assets/styles.css": ("styles.css", "text/css; charset=utf-8"),
            }
            asset_name, content_type = asset_map[path]
            asset_path = asset_dir / asset_name
            if not asset_path.exists():
                return self._write_error(HTTPStatus.NOT_FOUND, f"Missing asset {asset_name}")

            body = asset_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self._write_cors_headers()
            self.end_headers()
            self.wfile.write(body)

        def _authorize(self) -> bool:
            if not config.basic_auth_enabled:
                return True

            header = self.headers.get("Authorization", "")
            if not header.startswith("Basic "):
                self.send_response(HTTPStatus.UNAUTHORIZED)
                self.send_header("WWW-Authenticate", 'Basic realm="Facebrick"')
                self.end_headers()
                return False

            encoded = header.split(" ", 1)[1]
            try:
                decoded = base64.b64decode(encoded).decode("utf-8")
            except Exception:
                self.send_response(HTTPStatus.UNAUTHORIZED)
                self.end_headers()
                return False

            username, _, password = decoded.partition(":")
            if username != config.basic_auth_user or password != config.basic_auth_password:
                self.send_response(HTTPStatus.UNAUTHORIZED)
                self.send_header("WWW-Authenticate", 'Basic realm="Facebrick"')
                self.end_headers()
                return False
            return True

        def _require_safe_origin_for_write(self) -> None:
            origin = self.headers.get("Origin")
            if not origin:
                return

            normalized_origin = origin.rstrip("/")
            if config.allowed_origins:
                if normalized_origin in config.allowed_origins:
                    return
                raise FinOpsServiceError(f"Origin '{normalized_origin}' is not allowed.")

            host = self.headers.get("Host")
            if host and normalized_origin.endswith(host):
                return
            raise FinOpsServiceError(
                "Cross-origin writes are blocked by default. Configure FACEBRICK_ALLOWED_ORIGINS if you need them."
            )

        def _write_json(self, payload: object, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self._write_cors_headers()
            self.end_headers()
            self.wfile.write(body)

        def _write_error(self, status: HTTPStatus, message: str) -> None:
            self._write_json({"error": message}, status=status)

        def _write_cors_headers(self) -> None:
            origin = self.headers.get("Origin")
            if not origin:
                return
            normalized_origin = origin.rstrip("/")
            if config.allowed_origins and normalized_origin in config.allowed_origins:
                self.send_header("Access-Control-Allow-Origin", normalized_origin)
                self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

        def _read_json_body(self) -> Dict[str, object]:
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length == 0:
                return {}
            return json.loads(self.rfile.read(content_length).decode("utf-8"))

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer((config.bind_host, config.port), FacebrickHandler)
    print(
        f"Facebrick listening on http://{config.bind_host}:{config.port} "
        f"(db={config.redacted_database_url}, pricing={config.pricing_file})"
    )
    server.serve_forever()
