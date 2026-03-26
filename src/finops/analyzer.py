from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from src.entities import Cluster, ClusterSpec, Job, Run

from .models import CostInsight, FinOpsReport, JobCostSummary, PricingRate, RunCost

MS_PER_HOUR = 3_600_000


@dataclass(frozen=True)
class FinOpsConfig:
    currency: str = "USD"
    include_driver: bool = True
    autoscale_strategy: str = "average"
    expensive_job_share_threshold: float = 0.35
    top_n_insights: int = 3


@dataclass
class JobCostAccumulator:
    job_name: str
    total_cost: float = 0.0
    run_count: int = 0
    last_run_time: Optional[int] = None


class FinOpsAnalyzer:
    def __init__(
        self,
        pricing_rates: Iterable[PricingRate],
        clusters: Optional[Iterable[Cluster]] = None,
        config: Optional[FinOpsConfig] = None,
    ) -> None:
        self._pricing_by_node_type = {rate.node_type_id: rate for rate in pricing_rates}
        self._clusters_by_id = {
            cluster.cluster_id: cluster for cluster in clusters or []
        }
        self._config = config or FinOpsConfig()

    def estimate_run_cost(self, run: Run) -> Optional[RunCost]:
        if not run.duration_ms or run.duration_ms <= 0:
            return None

        cluster_spec, source = self._resolve_cluster_spec(run)
        if cluster_spec is None or not cluster_spec.node_type_id:
            return None

        pricing_rate = self._pricing_by_node_type.get(cluster_spec.node_type_id)
        if pricing_rate is None:
            return None

        billable_nodes = self._billable_nodes(cluster_spec)
        hourly_rate = pricing_rate.total_rate_per_hour * billable_nodes
        estimated_cost = hourly_rate * (run.duration_ms / MS_PER_HOUR)

        return RunCost(
            run_id=run.run_id,
            job_id=run.job_id,
            currency=pricing_rate.currency,
            estimated_cost=estimated_cost,
            duration_ms=run.duration_ms,
            billable_nodes=billable_nodes,
            hourly_rate=hourly_rate,
            source=source,
        )

    def build_report(
        self,
        jobs: Iterable[Job],
        runs: Iterable[Run],
        window_start_ms: Optional[int] = None,
    ) -> FinOpsReport:
        jobs_by_id = {job.job_id: job for job in jobs}
        run_costs: List[RunCost] = []
        summaries: Dict[int, JobCostAccumulator] = {}

        for run in runs:
            if (
                window_start_ms is not None
                and run.start_time is not None
                and run.start_time < window_start_ms
            ):
                continue

            run_cost = self.estimate_run_cost(run)
            if run_cost is None or run_cost.job_id is None:
                continue

            run_costs.append(run_cost)
            summary = summaries.setdefault(
                run_cost.job_id,
                JobCostAccumulator(
                    job_name=jobs_by_id[run_cost.job_id].name
                    if run_cost.job_id in jobs_by_id
                    else f"job-{run_cost.job_id}"
                ),
            )
            summary.total_cost += run_cost.estimated_cost
            summary.run_count += 1
            last_run_time = summary.last_run_time
            if run.start_time is not None and (
                last_run_time is None or run.start_time > last_run_time
            ):
                summary.last_run_time = run.start_time

        total_cost = sum(run_cost.estimated_cost for run_cost in run_costs)
        currency = run_costs[0].currency if run_costs else self._config.currency
        job_summaries = self._build_job_summaries(summaries, total_cost, currency)
        insights = self._build_insights(job_summaries, total_cost, currency)

        return FinOpsReport(
            currency=currency,
            total_cost=total_cost,
            run_costs=sorted(
                run_costs, key=lambda item: item.estimated_cost, reverse=True
            ),
            job_summaries=job_summaries,
            insights=insights,
        )

    def _resolve_cluster_spec(self, run: Run) -> tuple[Optional[ClusterSpec], str]:
        if run.cluster_spec is not None:
            return run.cluster_spec, "run.cluster_spec"

        if run.cluster_instance and run.cluster_instance.cluster_id:
            cluster = self._clusters_by_id.get(run.cluster_instance.cluster_id)
            if cluster is not None:
                return self._cluster_to_spec(cluster), "cluster_lookup"

        return None, "unresolved"

    def _billable_nodes(self, cluster_spec: ClusterSpec) -> float:
        worker_count = self._resolve_worker_count(cluster_spec)
        if self._config.include_driver:
            return worker_count + 1
        return worker_count

    def _resolve_worker_count(self, cluster_spec: ClusterSpec) -> float:
        if cluster_spec.num_workers is not None:
            return float(cluster_spec.num_workers)

        autoscale = cluster_spec.autoscale
        if autoscale is None:
            return 0.0

        if self._config.autoscale_strategy == "max":
            return float(autoscale.max_workers)
        if self._config.autoscale_strategy == "min":
            return float(autoscale.min_workers)
        return (autoscale.min_workers + autoscale.max_workers) / 2

    def _build_job_summaries(
        self,
        summaries: Dict[int, JobCostAccumulator],
        total_cost: float,
        currency: str,
    ) -> List[JobCostSummary]:
        job_summaries: List[JobCostSummary] = []
        for job_id, summary in summaries.items():
            job_total_cost = summary.total_cost
            run_count = summary.run_count
            cost_share = (job_total_cost / total_cost) if total_cost else 0.0
            avg_cost_per_run = (job_total_cost / run_count) if run_count else 0.0
            job_summaries.append(
                JobCostSummary(
                    job_id=job_id,
                    job_name=summary.job_name,
                    currency=currency,
                    total_cost=job_total_cost,
                    cost_share=cost_share,
                    run_count=run_count,
                    avg_cost_per_run=avg_cost_per_run,
                    last_run_time=summary.last_run_time,
                )
            )

        return sorted(job_summaries, key=lambda item: item.total_cost, reverse=True)

    def _build_insights(
        self,
        job_summaries: List[JobCostSummary],
        total_cost: float,
        currency: str,
    ) -> List[CostInsight]:
        insights: List[CostInsight] = []
        if total_cost <= 0:
            return insights

        for summary in job_summaries[: self._config.top_n_insights]:
            share = summary.cost_share
            insights.append(
                CostInsight(
                    kind="expensive_job",
                    subject_type="job",
                    subject_key=str(summary.job_id),
                    message=(
                        f"Job '{summary.job_name}' accounts for {share:.1%} of total cost "
                        f"with {summary.total_cost:.2f} {currency}."
                    ),
                    metric_value=summary.total_cost,
                    currency=currency,
                )
            )

            if share >= self._config.expensive_job_share_threshold:
                insights.append(
                    CostInsight(
                        kind="dominant_cost_share",
                        subject_type="job",
                        subject_key=str(summary.job_id),
                        message=(
                            f"Job '{summary.job_name}' exceeds the configured cost share threshold "
                            f"at {share:.1%} of total spend."
                        ),
                        metric_value=share,
                        currency=currency,
                    )
                )

        return insights

    @staticmethod
    def _cluster_to_spec(cluster: Cluster) -> ClusterSpec:
        return ClusterSpec(
            spark_version=cluster.spark_version,
            node_type_id=cluster.node_type_id,
            driver_node_type_id=cluster.driver_node_type_id,
            num_workers=cluster.num_workers,
            autoscale=cluster.autoscale,
            autotermination_minutes=cluster.autotermination_minutes,
        )
