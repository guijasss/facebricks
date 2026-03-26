from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ClusterInstance:
    cluster_id: Optional[str] = None
    spark_context_id: Optional[str] = None
