from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from src.entities import Cluster, ClusterSpec, Job, JobClusterSpec, Run

from src.finops.models import (
    CostInsight,
    FinOpsReport,
    JobCostSummary,
    NamedCostSummary,
    PricingRate,
    RunCost,
)

MS_PER_HOUR = 3_600_000


@dataclass(frozen=True)
class FinOpsConfig:
    currency: str = "USD"
    include_driver: bool = True
    autoscale_strategy: str = "average"
    expensive_job_share_threshold: float = 0.35
    top_n_insights: int = 3
    top_n_dataset_insights: int = 3
    metadata_delimiter: str = ","


PIPELINE_TAG_KEYS = (
    "pipeline",
    "pipeline_name",
    "pipeline_id",
    "dlt_pipeline",
    "dlt_pipeline_name",
)
TABLE_TAG_KEYS = (
    "table",
    "tables",
    "table_name",
    "table_names",
    "dataset",
    "datasets",
    "maintained_table",
    "maintained_tables",
    "output_table",
    "output_tables",
)
TABLE_TAG_SEPARATORS = (",", ";", "\n")


@dataclass
class JobCostAccumulator:
    job_name: str
    total_cost: float = 0.0
    run_count: int = 0
    last_run_time: Optional[int] = None


@dataclass
class NamedAccumulator:
    label: str
    total_cost: float = 0.0
    run_count: int = 0
    attribution_count: int = 0
    last_seen_time: Optional[int] = None


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

    def estimate_run_cost(self, run: Run, job: Optional[Job] = None) -> Optional[RunCost]:
        if not run.duration_ms or run.duration_ms <= 0:
            return None

        cluster_spec, source = self._resolve_cluster_spec(run, job=job)
        if cluster_spec is None or not cluster_spec.node_type_id:
            return None

        worker_pricing_rate = self._pricing_by_node_type.get(cluster_spec.node_type_id)
        if worker_pricing_rate is None:
            return None

        worker_count = self._resolve_worker_count(cluster_spec)
        hourly_rate = worker_pricing_rate.total_rate_per_hour * worker_count
        billable_nodes = worker_count
        if self._config.include_driver:
            driver_node_type_id = (
                cluster_spec.driver_node_type_id or cluster_spec.node_type_id
            )
            driver_pricing_rate = self._pricing_by_node_type.get(driver_node_type_id)
            if driver_pricing_rate is None:
                return None
            hourly_rate += driver_pricing_rate.total_rate_per_hour
            billable_nodes += 1
        estimated_cost = hourly_rate * (run.duration_ms / MS_PER_HOUR)

        return RunCost(
            run_id=run.run_id,
            job_id=run.job_id,
            currency=worker_pricing_rate.currency,
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
        analysis_window_days: Optional[int] = None,
    ) -> FinOpsReport:
        jobs_by_id = {job.job_id: job for job in jobs}
        run_costs: List[RunCost] = []
        summaries: Dict[int, JobCostAccumulator] = {}
        pipeline_summaries: Dict[str, NamedAccumulator] = {}
        table_summaries: Dict[str, NamedAccumulator] = {}

        for run in runs:
            if (
                window_start_ms is not None
                and run.start_time is not None
                and run.start_time < window_start_ms
            ):
                continue

            run_cost = self.estimate_run_cost(run, job=jobs_by_id.get(run.job_id))
            if run_cost is None or run_cost.job_id is None:
                continue

            run_costs.append(run_cost)
            job = jobs_by_id.get(run_cost.job_id)
            summary = summaries.setdefault(
                run_cost.job_id,
                JobCostAccumulator(
                    job_name=job.name if job is not None else f"job-{run_cost.job_id}"
                ),
            )
            summary.total_cost += run_cost.estimated_cost
            summary.run_count += 1
            last_run_time = summary.last_run_time
            if run.start_time is not None and (
                last_run_time is None or run.start_time > last_run_time
            ):
                summary.last_run_time = run.start_time

            pipeline_keys = self._extract_pipeline_keys(run, job)
            self._distribute_named_cost(
                pipeline_summaries,
                pipeline_keys,
                run_cost.estimated_cost,
                run.start_time,
            )

            table_keys = self._extract_table_keys(run, job)
            self._distribute_named_cost(
                table_summaries,
                table_keys,
                run_cost.estimated_cost,
                run.start_time,
            )

        total_cost = sum(run_cost.estimated_cost for run_cost in run_costs)
        currency = run_costs[0].currency if run_costs else self._config.currency
        job_summaries = self._build_job_summaries(summaries, total_cost, currency)
        pipeline_cost_summaries = self._build_named_summaries(
            pipeline_summaries, total_cost, currency, analysis_window_days
        )
        table_cost_summaries = self._build_named_summaries(
            table_summaries, total_cost, currency, analysis_window_days
        )
        insights = self._build_insights(
            job_summaries,
            pipeline_cost_summaries,
            table_cost_summaries,
            total_cost,
            currency,
        )

        return FinOpsReport(
            currency=currency,
            total_cost=total_cost,
            run_costs=sorted(
                run_costs, key=lambda item: item.estimated_cost, reverse=True
            ),
            job_summaries=job_summaries,
            pipeline_summaries=pipeline_cost_summaries,
            table_summaries=table_cost_summaries,
            insights=insights,
        )

    def _resolve_cluster_spec(
        self, run: Run, job: Optional[Job] = None
    ) -> tuple[Optional[ClusterSpec], str]:
        if run.cluster_spec is not None:
            return run.cluster_spec, "run.cluster_spec"

        if run.cluster_instance and run.cluster_instance.cluster_id:
            cluster = self._clusters_by_id.get(run.cluster_instance.cluster_id)
            if cluster is not None:
                return self._cluster_to_spec(cluster), "cluster_lookup"

        if job is not None:
            job_cluster_spec = self._resolve_job_cluster_spec(job)
            if job_cluster_spec is not None:
                return job_cluster_spec.new_cluster, "job.job_clusters"

        return None, "unresolved"

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

    def _resolve_job_cluster_spec(self, job: Job) -> Optional[JobClusterSpec]:
        if not job.job_clusters:
            return None

        cluster_refs = {
            task.cluster_ref
            for task in job.tasks or []
            if task.cluster_ref is not None
        }
        if len(cluster_refs) != 1:
            return None

        cluster_ref = next(iter(cluster_refs))
        if cluster_ref is None:
            return None

        for cluster_spec in job.job_clusters:
            if cluster_spec.job_cluster_key == cluster_ref:
                return cluster_spec
        return None

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
        pipeline_summaries: List[NamedCostSummary],
        table_summaries: List[NamedCostSummary],
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

        for summary in pipeline_summaries[: self._config.top_n_insights]:
            insights.append(
                CostInsight(
                    kind="expensive_pipeline",
                    subject_type="pipeline",
                    subject_key=summary.key,
                    message=(
                        f"Pipeline '{summary.label}' accounts for {summary.cost_share:.1%} "
                        f"of total cost with {summary.total_cost:.2f} {currency}."
                    ),
                    metric_value=summary.total_cost,
                    currency=currency,
                )
            )

        for summary in table_summaries[: self._config.top_n_dataset_insights]:
            insights.append(
                CostInsight(
                    kind="expensive_table",
                    subject_type="table",
                    subject_key=summary.key,
                    message=(
                        f"Table '{summary.label}' costs {summary.cost_per_day:.2f} "
                        f"{currency} per day to maintain in this window."
                    ),
                    metric_value=summary.cost_per_day,
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

    def _extract_pipeline_keys(self, run: Run, job: Optional[Job]) -> List[str]:
        return self._unique_preserve_order(
            self._extract_tag_values(run.tags, PIPELINE_TAG_KEYS)
            + self._extract_tag_values(job.tags if job is not None else None, PIPELINE_TAG_KEYS)
        )

    def _extract_table_keys(self, run: Run, job: Optional[Job]) -> List[str]:
        return self._unique_preserve_order(
            self._extract_tag_values(run.tags, TABLE_TAG_KEYS)
            + self._extract_tag_values(job.tags if job is not None else None, TABLE_TAG_KEYS)
        )

    def _extract_tag_values(
        self,
        tags: Optional[Dict[str, str]],
        candidate_keys: Sequence[str],
    ) -> List[str]:
        if not tags:
            return []

        normalized = {key.lower(): value for key, value in tags.items()}
        values: List[str] = []
        for key in candidate_keys:
            raw_value = normalized.get(key)
            if not raw_value:
                continue
            split_values = [raw_value]
            for separator in TABLE_TAG_SEPARATORS:
                expanded: List[str] = []
                for item in split_values:
                    expanded.extend(item.split(separator))
                split_values = expanded
            values.extend(value.strip() for value in split_values if value.strip())
        return values

    def _distribute_named_cost(
        self,
        accumulators: Dict[str, NamedAccumulator],
        keys: Sequence[str],
        total_cost: float,
        last_seen_time: Optional[int],
    ) -> None:
        normalized_keys = self._unique_preserve_order(keys)
        if not normalized_keys:
            return

        share = total_cost / len(normalized_keys)
        for key in normalized_keys:
            accumulator = accumulators.setdefault(key, NamedAccumulator(label=key))
            accumulator.total_cost += share
            accumulator.run_count += 1
            accumulator.attribution_count += 1
            if last_seen_time is not None and (
                accumulator.last_seen_time is None
                or last_seen_time > accumulator.last_seen_time
            ):
                accumulator.last_seen_time = last_seen_time

    def _build_named_summaries(
        self,
        summaries: Dict[str, NamedAccumulator],
        total_cost: float,
        currency: str,
        analysis_window_days: Optional[int],
    ) -> List[NamedCostSummary]:
        named_summaries: List[NamedCostSummary] = []
        window_days = max(1, analysis_window_days or 1)
        for key, summary in summaries.items():
            avg_cost_per_run = (
                summary.total_cost / summary.run_count if summary.run_count else 0.0
            )
            cost_per_day = summary.total_cost / window_days
            named_summaries.append(
                NamedCostSummary(
                    key=key,
                    label=summary.label,
                    currency=currency,
                    total_cost=summary.total_cost,
                    cost_share=(summary.total_cost / total_cost) if total_cost else 0.0,
                    run_count=summary.run_count,
                    avg_cost_per_run=avg_cost_per_run,
                    cost_per_day=cost_per_day,
                    attribution_count=summary.attribution_count,
                    last_seen_time=summary.last_seen_time,
                )
            )
        return sorted(named_summaries, key=lambda item: item.total_cost, reverse=True)

    @staticmethod
    def _unique_preserve_order(values: Sequence[str]) -> List[str]:
        seen: set[str] = set()
        ordered: List[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            ordered.append(value)
        return ordered
