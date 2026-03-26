from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .cluster_spec import Autoscale


@dataclass(frozen=True)
class Cluster:
    cluster_id: str
    cluster_name: Optional[str] = None
    spark_version: Optional[str] = None
    node_type_id: Optional[str] = None
    driver_node_type_id: Optional[str] = None
    num_workers: Optional[int] = None
    autoscale: Optional[Autoscale] = None
    autotermination_minutes: Optional[int] = None
