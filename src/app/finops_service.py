from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Literal, Optional, TypedDict

from src.entities import Run
from src.finops import FinOpsAnalyzer, FinOpsConfig, PricingRate, RunCost

from src.app.config import AppConfig
from src.app.databricks import DatabricksClient, DatabricksCredentials
from src.app.storage import Storage, StorageProtocol


class FinOpsServiceError(RuntimeError):
    pass


class SummaryPayload(TypedDict):
    window_days: int
    currency: str
    total_cost: float
    run_count: int
    job_count: int
    pipeline_count: int
    table_count: int
    avg_cost_per_run: float
    most_expensive_job: Optional[str]
    most_expensive_pipeline: Optional[str]
    most_expensive_table: Optional[str]
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


class NamedCostPayload(TypedDict):
    key: str
    label: str
    total_cost: float
    cost_share: float
    run_count: int
    avg_cost_per_run: float
    cost_per_day: float
    attribution_count: int
    last_seen_time: Optional[int]


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
    top_pipelines: List[NamedCostPayload]
    top_tables: List[NamedCostPayload]
    recent_runs: List[RunPayload]
    insights: List[InsightPayload]
    coverage: CoveragePayload


class SyncPayload(TypedDict):
    saved_jobs: int
    saved_runs: int
    saved_clusters: int
    last_sync_at: str


class ClusterPricingEntryPayload(TypedDict):
    node_type_id: str
    dbus_per_hour: float
    plan: Literal["premium", "enterprise"]
    jobs_rate_per_hour: float
    all_purpose_rate_per_hour: float


class ClusterPricingConfigPayload(TypedDict):
    currency: str
    workload_type: str
    cluster_node_types: List[str]
    entries: List[ClusterPricingEntryPayload]
    last_refreshed_at: Optional[str]


class FinOpsService:
    def __init__(
        self, config: AppConfig, storage: Optional[StorageProtocol] = None
    ) -> None:
        self._config = config
        self._storage: StorageProtocol = storage or Storage(config.database_url)
        self._databricks_client: Optional[DatabricksClient] = None

    def sync(self, runs_limit: int = 250) -> SyncPayload:
        if not self._config.databricks_configured:
            raise FinOpsServiceError(
                "Databricks is not configured. Set it in config/facebrick.config.json or with DATABRICKS_HOST and DATABRICKS_TOKEN."
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
        pricing_rates = self._load_pricing_rates()
        analyzer = FinOpsAnalyzer(
            pricing_rates=pricing_rates, clusters=clusters, config=FinOpsConfig()
        )
        report = analyzer.build_report(
            jobs=jobs,
            runs=runs,
            window_start_ms=window_start_ms,
            analysis_window_days=window_days,
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
                "pipeline_count": len(report.pipeline_summaries),
                "table_count": len(report.table_summaries),
                "avg_cost_per_run": round(report.total_cost / len(report.run_costs), 2)
                if report.run_costs
                else 0.0,
                "most_expensive_job": report.job_summaries[0].job_name
                if report.job_summaries
                else None,
                "most_expensive_pipeline": report.pipeline_summaries[0].label
                if report.pipeline_summaries
                else None,
                "most_expensive_table": report.table_summaries[0].label
                if report.table_summaries
                else None,
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
            "top_pipelines": [
                {
                    "key": summary.key,
                    "label": summary.label,
                    "total_cost": round(summary.total_cost, 2),
                    "cost_share": round(summary.cost_share, 4),
                    "run_count": summary.run_count,
                    "avg_cost_per_run": round(summary.avg_cost_per_run, 2),
                    "cost_per_day": round(summary.cost_per_day, 2),
                    "attribution_count": summary.attribution_count,
                    "last_seen_time": summary.last_seen_time,
                }
                for summary in report.pipeline_summaries
            ],
            "top_tables": [
                {
                    "key": summary.key,
                    "label": summary.label,
                    "total_cost": round(summary.total_cost, 2),
                    "cost_share": round(summary.cost_share, 4),
                    "run_count": summary.run_count,
                    "avg_cost_per_run": round(summary.avg_cost_per_run, 2),
                    "cost_per_day": round(summary.cost_per_day, 2),
                    "attribution_count": summary.attribution_count,
                    "last_seen_time": summary.last_seen_time,
                }
                for summary in report.table_summaries
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

    def get_pipelines(self, window_days: int = 30) -> List[NamedCostPayload]:
        dashboard: DashboardPayload = self.get_dashboard(window_days=window_days)
        return dashboard["top_pipelines"]

    def get_tables(self, window_days: int = 30) -> List[NamedCostPayload]:
        dashboard: DashboardPayload = self.get_dashboard(window_days=window_days)
        return dashboard["top_tables"]

    def get_insights(self, window_days: int = 30) -> List[InsightPayload]:
        dashboard: DashboardPayload = self.get_dashboard(window_days=window_days)
        return dashboard["insights"]

    def get_cluster_pricing_config(
        self, refresh: bool = False
    ) -> ClusterPricingConfigPayload:
        persisted = self._read_pricing_config_metadata()
        cluster_node_types = list(persisted["cluster_node_types"])
        last_refreshed_at = persisted["last_refreshed_at"]

        if refresh:
            cluster_node_types = self._load_cluster_node_types_from_databricks()
            last_refreshed_at = datetime.now(timezone.utc).isoformat()
            persisted["cluster_node_types"] = cluster_node_types
            persisted["last_refreshed_at"] = last_refreshed_at
            self._write_pricing_config_metadata(persisted)

        entries = [
            _entry_payload(node_type_id, persisted["entries"].get(node_type_id))
            for node_type_id in cluster_node_types
        ]
        configured_only = [
            _entry_payload(node_type_id, entry)
            for node_type_id, entry in sorted(persisted["entries"].items())
            if node_type_id not in cluster_node_types
        ]

        return {
            "currency": "USD",
            "workload_type": "jobs",
            "cluster_node_types": cluster_node_types,
            "entries": entries + configured_only,
            "last_refreshed_at": last_refreshed_at,
        }

    def save_cluster_pricing_config(
        self, entries: List[dict[str, object]]
    ) -> ClusterPricingConfigPayload:
        persisted = self._read_pricing_config_metadata()
        normalized_entries: Dict[str, dict[str, object]] = {}
        for raw_entry in entries:
            node_type_id = str(raw_entry.get("node_type_id", "")).strip()
            if not node_type_id:
                continue
            dbus_per_hour = float(raw_entry.get("dbus_per_hour", 0.0))
            if dbus_per_hour < 0:
                raise FinOpsServiceError("DBUs per hour must be greater than or equal to 0.")
            plan = str(raw_entry.get("plan", "premium")).strip().lower()
            if plan not in {"premium", "enterprise"}:
                raise FinOpsServiceError(
                    f"Unsupported plan '{plan}' for node type '{node_type_id}'."
                )
            normalized_entries[node_type_id] = {
                "dbus_per_hour": dbus_per_hour,
                "plan": plan,
            }

        persisted["entries"] = normalized_entries
        self._write_pricing_config_metadata(persisted)
        return self.get_cluster_pricing_config(refresh=False)

    def _client(self) -> DatabricksClient:
        if self._databricks_client is None:
            self._databricks_client = DatabricksClient(
                DatabricksCredentials(
                    host=self._config.databricks_host or "",
                    token=self._config.databricks_token or "",
                )
            )
        return self._databricks_client

    def _load_pricing_rates(self) -> List[PricingRate]:
        persisted = self._read_pricing_config_metadata()
        pricing_rates: List[PricingRate] = []
        for node_type_id, entry in persisted["entries"].items():
            dbus_per_hour = float(entry.get("dbus_per_hour", 0.0))
            if dbus_per_hour <= 0:
                continue
            plan = str(entry.get("plan", "premium")).lower()
            pricing_rates.append(
                PricingRate(
                    node_type_id=node_type_id,
                    dbu_rate_per_hour=_jobs_rate(plan=plan, dbus_per_hour=dbus_per_hour),
                    currency="USD",
                )
            )
        return pricing_rates

    def _load_cluster_node_types_from_databricks(self) -> List[str]:
        if not self._config.databricks_configured:
            raise FinOpsServiceError(
                "Databricks is not configured. Set it in config/facebrick.config.json or with DATABRICKS_HOST and DATABRICKS_TOKEN."
            )
        if not self._config.databricks_sql_warehouse_id:
            raise FinOpsServiceError(
                "Databricks SQL warehouse is not configured. Set databricks.sql_warehouse_id or DATABRICKS_SQL_WAREHOUSE_ID."
            )
        return self._client().list_distinct_cluster_node_types(
            self._config.databricks_sql_warehouse_id
        )

    def _read_pricing_config_metadata(self) -> dict[str, object]:
        raw_value = self._storage.read_metadata("cluster_pricing_config")
        if not raw_value:
            return {
                "cluster_node_types": [],
                "entries": {},
                "last_refreshed_at": None,
            }

        payload = json.loads(raw_value)
        if not isinstance(payload, dict):
            raise FinOpsServiceError("Stored cluster pricing config is invalid.")

        raw_entries = payload.get("entries")
        raw_cluster_node_types = payload.get("cluster_node_types")
        entries = raw_entries if isinstance(raw_entries, dict) else {}
        cluster_node_types = (
            [str(item) for item in raw_cluster_node_types if str(item).strip()]
            if isinstance(raw_cluster_node_types, list)
            else []
        )
        return {
            "cluster_node_types": cluster_node_types,
            "entries": {
                str(node_type_id): dict(entry)
                for node_type_id, entry in entries.items()
                if isinstance(entry, dict)
            },
            "last_refreshed_at": _optional_str(payload.get("last_refreshed_at")),
        }

    def _write_pricing_config_metadata(self, payload: dict[str, object]) -> None:
        self._storage.write_metadata("cluster_pricing_config", json.dumps(payload))


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


def _jobs_rate(plan: str, dbus_per_hour: float) -> float:
    multiplier = 0.15 if plan == "premium" else 0.20
    return dbus_per_hour * multiplier


def _all_purpose_rate(plan: str, dbus_per_hour: float) -> float:
    multiplier = 0.55 if plan == "premium" else 0.65
    return dbus_per_hour * multiplier


def _entry_payload(
    node_type_id: str, persisted_entry: Optional[dict[str, object]]
) -> ClusterPricingEntryPayload:
    dbus_per_hour = (
        float(persisted_entry.get("dbus_per_hour", 0.0))
        if persisted_entry is not None
        else 0.0
    )
    plan = (
        str(persisted_entry.get("plan", "premium")).lower()
        if persisted_entry is not None
        else "premium"
    )
    if plan not in {"premium", "enterprise"}:
        plan = "premium"
    return {
        "node_type_id": node_type_id,
        "dbus_per_hour": dbus_per_hour,
        "plan": plan,  # type: ignore[return-value]
        "jobs_rate_per_hour": round(_jobs_rate(plan, dbus_per_hour), 4),
        "all_purpose_rate_per_hour": round(_all_purpose_rate(plan, dbus_per_hour), 4),
    }


def _optional_str(value: object) -> Optional[str]:
    return None if value in (None, "") else str(value)
