from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Autoscale:
    min_workers: int
    max_workers: int


@dataclass(frozen=True)
class ClusterSpec:
    spark_version: Optional[str] = None
    node_type_id: Optional[str] = None
    driver_node_type_id: Optional[str] = None
    num_workers: Optional[int] = None
    autoscale: Optional[Autoscale] = None
    autotermination_minutes: Optional[int] = None
