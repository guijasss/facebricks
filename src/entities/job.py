from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from .job_cluster_spec import JobClusterSpec
from .job_task import JobTask


@dataclass(frozen=True)
class Schedule:
    quartz_cron_expression: Optional[str] = None
    timezone_id: Optional[str] = None
    pause_status: Optional[str] = None


@dataclass(frozen=True)
class Job:
    job_id: int
    name: str
    creator_user_name: Optional[str] = None
    created_time: Optional[int] = None
    tags: Optional[Dict[str, str]] = None
    schedule: Optional[Schedule] = None
    max_concurrent_runs: Optional[int] = None
    tasks: Optional[List[JobTask]] = None
    job_clusters: Optional[List[JobClusterSpec]] = None
