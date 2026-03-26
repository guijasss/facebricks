from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from typing import Optional, Tuple


@dataclass(frozen=True)
class AppConfig:
    database_url: str
    pricing_file: Path
    bind_host: str = "127.0.0.1"
    port: int = 8000
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    allowed_origins: Tuple[str, ...] = ()
    basic_auth_user: Optional[str] = None
    basic_auth_password: Optional[str] = None

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
        database_url = os.getenv(
            "FACEBRICK_DATABASE_URL",
            "postgresql://facebrick:facebrick@127.0.0.1:5432/facebrick",
        )
        pricing_file = root_dir / os.getenv("FACEBRICK_PRICING_FILE", "config/pricing.example.json")
        bind_host = os.getenv("FACEBRICK_BIND", "127.0.0.1")
        port = int(os.getenv("FACEBRICK_PORT", "8000"))
        allowed_origins_raw = os.getenv("FACEBRICK_ALLOWED_ORIGINS", "")
        allowed_origins = tuple(
            origin.strip().rstrip("/")
            for origin in allowed_origins_raw.split(",")
            if origin.strip()
        )
        databricks_host = os.getenv("DATABRICKS_HOST")
        if databricks_host:
            databricks_host = databricks_host.rstrip("/")

        return cls(
            database_url=database_url,
            pricing_file=pricing_file,
            bind_host=bind_host,
            port=port,
            databricks_host=databricks_host,
            databricks_token=os.getenv("DATABRICKS_TOKEN"),
            allowed_origins=allowed_origins,
            basic_auth_user=os.getenv("FACEBRICK_BASIC_AUTH_USER"),
            basic_auth_password=os.getenv("FACEBRICK_BASIC_AUTH_PASSWORD"),
        )
