from __future__ import annotations

from pathlib import Path

from src.app.config import AppConfig
from src.app.http import run_server


def main() -> None:
    root_dir = Path(__file__).resolve().parent
    config = AppConfig.from_env(base_dir=root_dir)
    run_server(config=config, base_dir=root_dir)


if __name__ == "__main__":
    main()
