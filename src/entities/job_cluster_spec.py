from __future__ import annotations

from dataclasses import dataclass

from .cluster_spec import ClusterSpec


@dataclass(frozen=True)
class JobClusterSpec:
    job_cluster_key: str
    new_cluster: ClusterSpec
