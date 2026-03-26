from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from .cluster_instance import ClusterInstance
from .cluster_spec import ClusterSpec
from .run_state import RunState


@dataclass(frozen=True)
class Run:
    run_id: int
    job_id: Optional[int] = None
    run_name: Optional[str] = None
    creator_user_name: Optional[str] = None
    run_page_url: Optional[str] = None
    trigger: Optional[str] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    duration_ms: Optional[int] = None
    state: Optional[RunState] = None
    cluster_instance: Optional[ClusterInstance] = None
    cluster_spec: Optional[ClusterSpec] = None
    tags: Optional[Dict[str, str]] = None
