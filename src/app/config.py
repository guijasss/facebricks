from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from typing import Any, Optional, Tuple


@dataclass(frozen=True)
class AppConfig:
    database_url: str
    pricing_data: dict[str, object]
    bind_host: str = "127.0.0.1"
    port: int = 8000
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    databricks_sql_warehouse_id: Optional[str] = None
    allowed_origins: Tuple[str, ...] = ()
    basic_auth_user: Optional[str] = None
    basic_auth_password: Optional[str] = None
    config_file: Optional[Path] = None

    @property
    def databricks_configured(self) -> bool:
        return bool(self.databricks_host and self.databricks_token)

    @property
    def basic_auth_enabled(self) -> bool:
        return bool(self.basic_auth_user and self.basic_auth_password)

    @property
    def redacted_database_url(self) -> str:
        parsed = urlsplit(self.database_url)
        if "@" not in parsed.netloc or ":" not in parsed.netloc.split("@", 1)[0]:
            return self.database_url
        credentials, host = parsed.netloc.split("@", 1)
        username = credentials.split(":", 1)[0]
        return parsed._replace(netloc=f"{username}:***@{host}").geturl()

    @classmethod
    def from_env(cls, base_dir: Optional[Path] = None) -> "AppConfig":
        root_dir = base_dir or Path.cwd()
        config_file = _resolve_path(
            root_dir, os.getenv("FACEBRICK_CONFIG_FILE", "config/facebrick.config.json")
        )
        file_config = _load_json_config(config_file)

        database_url = os.getenv(
            "FACEBRICK_DATABASE_URL",
            str(
                file_config.get(
                    "database_url",
                    "postgresql://facebrick:facebrick@127.0.0.1:5432/facebrick",
                )
            ),
        )
        bind_host = os.getenv(
            "FACEBRICK_BIND", str(file_config.get("bind_host", "127.0.0.1"))
        )
        port = int(os.getenv("FACEBRICK_PORT", str(file_config.get("port", "8000"))))

        allowed_origins_raw = os.getenv("FACEBRICK_ALLOWED_ORIGINS")
        if allowed_origins_raw is None:
            allowed_origins = _tuple_of_origins(file_config.get("allowed_origins"))
        else:
            allowed_origins = _tuple_of_origins(allowed_origins_raw)

        databricks_config = file_config.get("databricks")
        databricks_host = os.getenv("DATABRICKS_HOST") or _nested_str(
            databricks_config, "host"
        )
        if databricks_host:
            databricks_host = databricks_host.rstrip("/")

        basic_auth_config = file_config.get("basic_auth")
        pricing_data = _pricing_payload(file_config.get("pricing"))

        return cls(
            database_url=database_url,
            pricing_data=pricing_data,
            bind_host=bind_host,
            port=port,
            databricks_host=databricks_host,
            databricks_token=os.getenv("DATABRICKS_TOKEN")
            or _nested_str(databricks_config, "token"),
            databricks_sql_warehouse_id=os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")
            or _nested_str(databricks_config, "sql_warehouse_id"),
            allowed_origins=allowed_origins,
            basic_auth_user=os.getenv("FACEBRICK_BASIC_AUTH_USER")
            or _nested_str(basic_auth_config, "user"),
            basic_auth_password=os.getenv("FACEBRICK_BASIC_AUTH_PASSWORD")
            or _nested_str(basic_auth_config, "password"),
            config_file=config_file if config_file.exists() else None,
        )


def _resolve_path(root_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return root_dir / path


def _load_json_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Facebrick config file must contain a top-level JSON object.")
    return payload


def _nested_str(value: object, key: str) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    item = value.get(key)
    if item in (None, ""):
        return None
    return str(item)


def _tuple_of_origins(value: object) -> Tuple[str, ...]:
    if isinstance(value, str):
        return tuple(
            origin.strip().rstrip("/")
            for origin in value.split(",")
            if origin.strip()
        )
    if isinstance(value, list):
        return tuple(
            str(origin).strip().rstrip("/")
            for origin in value
            if str(origin).strip()
        )
    return ()


def _pricing_payload(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    return {
        "currency": "USD",
        "rates": [
            {
                "node_type_id": "m5d.large",
                "dbu_rate_per_hour": 0.55,
                "infrastructure_rate_per_hour": 0.096,
            }
        ],
    }
