from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, TypedDict

from src.entities import Run
from src.finops import FinOpsAnalyzer, FinOpsConfig, PricingRate, RunCost

from .config import AppConfig
from .databricks import DatabricksClient, DatabricksCredentials
from .storage import Storage, StorageProtocol


class FinOpsServiceError(RuntimeError):
    pass


class SummaryPayload(TypedDict):
    window_days: int
    currency: str
    total_cost: float
    run_count: int
    job_count: int
    avg_cost_per_run: float
    last_sync_at: Optional[str]


class CostOverTimePoint(TypedDict):
    date: str
    total_cost: float
    run_count: int


class JobPayload(TypedDict):
    job_id: int
    job_name: str
    total_cost: float
    cost_share: float
    run_count: int
    avg_cost_per_run: float
    last_run_time: Optional[int]


class RunPayload(TypedDict):
    run_id: int
    job_id: Optional[int]
    job_name: str
    estimated_cost: float
    duration_ms: int
    start_time: Optional[int]
    run_page_url: Optional[str]
    currency: str
    source: str


class InsightPayload(TypedDict):
    kind: str
    subject_type: str
    subject_key: str
    message: str
    metric_value: float
    currency: Optional[str]


class CoveragePayload(TypedDict):
    total_runs: int
    costed_runs: int
    uncosted_runs: int
    priced_node_types: List[str]


class DashboardPayload(TypedDict):
    summary: SummaryPayload
    cost_over_time: List[CostOverTimePoint]
    top_jobs: List[JobPayload]
    recent_runs: List[RunPayload]
    insights: List[InsightPayload]
    coverage: CoveragePayload


class SyncPayload(TypedDict):
    saved_jobs: int
    saved_runs: int
    saved_clusters: int
    last_sync_at: str


class FinOpsService:
    def __init__(
        self, config: AppConfig, storage: Optional[StorageProtocol] = None
    ) -> None:
        self._config = config
        self._storage: StorageProtocol = storage or Storage(config.database_url)

    def sync(self, runs_limit: int = 250) -> SyncPayload:
        if not self._config.databricks_configured:
            raise FinOpsServiceError(
                "Databricks is not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN in the backend environment."
            )

        client = DatabricksClient(
            DatabricksCredentials(
                host=self._config.databricks_host or "",
                token=self._config.databricks_token or "",
            )
        )
        jobs = client.list_jobs()
        runs = client.list_runs(limit=runs_limit)
        cluster_ids = sorted(
            {
                run.cluster_instance.cluster_id
                for run in runs
                if run.cluster_instance and run.cluster_instance.cluster_id
            }
        )
        clusters = client.get_clusters(cluster_ids)

        saved_jobs = self._storage.save_jobs(jobs)
        saved_runs = self._storage.save_runs(runs)
        saved_clusters = self._storage.save_clusters(clusters)
        synced_at = datetime.now(timezone.utc).isoformat()
        self._storage.write_metadata("last_sync_at", synced_at)

        return {
            "saved_jobs": saved_jobs,
            "saved_runs": saved_runs,
            "saved_clusters": saved_clusters,
            "last_sync_at": synced_at,
        }

    def get_dashboard(
        self, window_days: int = 30, recent_runs_limit: int = 20
    ) -> DashboardPayload:
        cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
        window_start_ms = int(cutoff.timestamp() * 1000)
        jobs = self._storage.load_jobs()
        runs = self._storage.load_runs(window_start_ms=window_start_ms)
        clusters = self._storage.load_clusters()
        pricing_rates = _load_pricing_rates(self._config.pricing_file)
        analyzer = FinOpsAnalyzer(
            pricing_rates=pricing_rates, clusters=clusters, config=FinOpsConfig()
        )
        report = analyzer.build_report(
            jobs=jobs, runs=runs, window_start_ms=window_start_ms
        )

        jobs_by_id = {job.job_id: job for job in jobs}
        runs_by_id = {run.run_id: run for run in runs}
        recent_run_rows: List[RunPayload] = []
        for run_cost in sorted(
            report.run_costs,
            key=lambda item: runs_by_id[item.run_id].start_time or 0,
            reverse=True,
        )[:recent_runs_limit]:
            run = runs_by_id[run_cost.run_id]
            job = (
                jobs_by_id.get(run_cost.job_id) if run_cost.job_id is not None else None
            )
            recent_run_rows.append(
                {
                    "run_id": run_cost.run_id,
                    "job_id": run_cost.job_id,
                    "job_name": job.name
                    if job is not None
                    else f"job-{run_cost.job_id}",
                    "estimated_cost": round(run_cost.estimated_cost, 2),
                    "duration_ms": run_cost.duration_ms,
                    "start_time": run.start_time,
                    "run_page_url": run.run_page_url,
                    "currency": run_cost.currency,
                    "source": run_cost.source,
                }
            )

        coverage: CoveragePayload = {
            "total_runs": len(runs),
            "costed_runs": len(report.run_costs),
            "uncosted_runs": max(0, len(runs) - len(report.run_costs)),
            "priced_node_types": sorted({rate.node_type_id for rate in pricing_rates}),
        }

        return {
            "summary": {
                "window_days": window_days,
                "currency": report.currency,
                "total_cost": round(report.total_cost, 2),
                "run_count": len(report.run_costs),
                "job_count": len(report.job_summaries),
                "avg_cost_per_run": round(report.total_cost / len(report.run_costs), 2)
                if report.run_costs
                else 0.0,
                "last_sync_at": self._storage.read_metadata("last_sync_at"),
            },
            "cost_over_time": _build_cost_over_time(report.run_costs, runs_by_id),
            "top_jobs": [
                {
                    "job_id": summary.job_id,
                    "job_name": summary.job_name,
                    "total_cost": round(summary.total_cost, 2),
                    "cost_share": round(summary.cost_share, 4),
                    "run_count": summary.run_count,
                    "avg_cost_per_run": round(summary.avg_cost_per_run, 2),
                    "last_run_time": summary.last_run_time,
                }
                for summary in report.job_summaries
            ],
            "recent_runs": recent_run_rows,
            "insights": [
                InsightPayload(**asdict(insight)) for insight in report.insights
            ],
            "coverage": coverage,
        }

    def get_summary(self, window_days: int = 30) -> SummaryPayload:
        dashboard: DashboardPayload = self.get_dashboard(window_days=window_days)
        return dashboard["summary"]

    def get_jobs(self, window_days: int = 30) -> List[JobPayload]:
        dashboard: DashboardPayload = self.get_dashboard(window_days=window_days)
        return dashboard["top_jobs"]

    def get_runs(self, window_days: int = 30, limit: int = 20) -> List[RunPayload]:
        dashboard: DashboardPayload = self.get_dashboard(
            window_days=window_days, recent_runs_limit=limit
        )
        return dashboard["recent_runs"]

    def get_insights(self, window_days: int = 30) -> List[InsightPayload]:
        dashboard: DashboardPayload = self.get_dashboard(window_days=window_days)
        return dashboard["insights"]


def _build_cost_over_time(
    run_costs: Iterable[RunCost], runs_by_id: Dict[int, Run]
) -> List[CostOverTimePoint]:
    buckets: Dict[str, CostOverTimePoint] = {}
    for run_cost in run_costs:
        run = runs_by_id[run_cost.run_id]
        if run.start_time is None:
            continue
        day = datetime.fromtimestamp(run.start_time / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )
        bucket = buckets.setdefault(
            day, {"date": day, "total_cost": 0.0, "run_count": 0}
        )
        bucket["total_cost"] = float(bucket["total_cost"]) + run_cost.estimated_cost
        bucket["run_count"] = int(bucket["run_count"]) + 1

    return [
        {
            "date": day,
            "total_cost": round(float(bucket["total_cost"]), 2),
            "run_count": int(bucket["run_count"]),
        }
        for day, bucket in sorted(buckets.items())
    ]


def _load_pricing_rates(pricing_file: Path) -> List[PricingRate]:
    if not pricing_file.exists():
        raise FinOpsServiceError(
            f"Pricing file '{pricing_file}' does not exist. Set FACEBRICK_PRICING_FILE to a valid JSON file."
        )

    payload = json.loads(pricing_file.read_text(encoding="utf-8"))
    raw_rates = payload.get("rates")
    if not isinstance(raw_rates, list):
        raise FinOpsServiceError("Pricing file must contain a top-level 'rates' array.")

    currency = str(payload.get("currency", "USD"))
    pricing_rates: List[PricingRate] = []
    for raw_rate in raw_rates:
        if not isinstance(raw_rate, dict):
            continue
        pricing_rates.append(
            PricingRate(
                node_type_id=str(raw_rate["node_type_id"]),
                dbu_rate_per_hour=float(raw_rate["dbu_rate_per_hour"]),
                currency=str(raw_rate.get("currency", currency)),
                infrastructure_rate_per_hour=float(
                    raw_rate.get("infrastructure_rate_per_hour", 0.0)
                ),
            )
        )

    if not pricing_rates:
        raise FinOpsServiceError(
            "Pricing file does not define any usable pricing rates."
        )
    return pricing_rates
