from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class PricingRate:
    node_type_id: str
    dbu_rate_per_hour: float
    currency: str = "USD"
    infrastructure_rate_per_hour: float = 0.0

    @property
    def total_rate_per_hour(self) -> float:
        return self.dbu_rate_per_hour + self.infrastructure_rate_per_hour


@dataclass(frozen=True)
class RunCost:
    run_id: int
    job_id: Optional[int]
    currency: str
    estimated_cost: float
    duration_ms: int
    billable_nodes: float
    hourly_rate: float
    source: str


@dataclass(frozen=True)
class JobCostSummary:
    job_id: int
    job_name: str
    currency: str
    total_cost: float
    cost_share: float
    run_count: int
    avg_cost_per_run: float
    last_run_time: Optional[int] = None


@dataclass(frozen=True)
class NamedCostSummary:
    key: str
    label: str
    currency: str
    total_cost: float
    cost_share: float
    run_count: int
    avg_cost_per_run: float
    cost_per_day: float
    attribution_count: int
    last_seen_time: Optional[int] = None


@dataclass(frozen=True)
class CostInsight:
    kind: str
    subject_type: str
    subject_key: str
    message: str
    metric_value: float
    currency: Optional[str] = None


@dataclass(frozen=True)
class FinOpsReport:
    currency: str
    total_cost: float
    run_costs: List[RunCost]
    job_summaries: List[JobCostSummary]
    pipeline_summaries: List[NamedCostSummary]
    table_summaries: List[NamedCostSummary]
    insights: List[CostInsight]
