from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class JobTask:
    task_key: str
    notebook_path: Optional[str] = None
    cluster_ref: Optional[str] = None
