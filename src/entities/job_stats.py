from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class JobStats:
    job_id: int
    window_days: int
    run_count: int
    success_count: int
    failure_count: int
    failure_rate: float
    avg_duration_ms: Optional[int] = None
    p95_duration_ms: Optional[int] = None
    last_run_time: Optional[int] = None
    last_success_time: Optional[int] = None
    last_failure_time: Optional[int] = None
    estimated_cost: Optional[float] = None
    unstable_flag: Optional[bool] = None
    orphan_flag: Optional[bool] = None
