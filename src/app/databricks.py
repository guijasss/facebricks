from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.entities import (
    Autoscale,
    Cluster,
    ClusterInstance,
    ClusterSpec,
    Job,
    JobTask,
    Run,
    RunState,
)
from src.entities.job import Schedule


class DatabricksClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabricksCredentials:
    host: str
    token: str


class DatabricksClient:
    def __init__(self, credentials: DatabricksCredentials, timeout_seconds: int = 30) -> None:
        self._credentials = credentials
        self._timeout_seconds = timeout_seconds

    def list_jobs(self) -> List[Job]:
        jobs: List[Job] = []
        page_token: Optional[str] = None

        while True:
            payload = self._get_json(
                "/api/2.2/jobs/list",
                {
                    "limit": 100,
                    "expand_tasks": "true",
                    **({"page_token": page_token} if page_token else {}),
                },
            )
            for raw_job in payload.get("jobs", []):
                jobs.append(_parse_job(raw_job))

            page_token = payload.get("next_page_token")
            if not page_token:
                return jobs

    def list_runs(self, limit: int = 250) -> List[Run]:
        runs: List[Run] = []
        page_token: Optional[str] = None

        while len(runs) < limit:
            page_limit = min(100, limit - len(runs))
            payload = self._get_json(
                "/api/2.2/jobs/runs/list",
                {
                    "limit": page_limit,
                    "completed_only": "true",
                    **({"page_token": page_token} if page_token else {}),
                },
            )
            for raw_run in payload.get("runs", []):
                runs.append(_parse_run(raw_run))
                if len(runs) >= limit:
                    break

            page_token = payload.get("next_page_token")
            if not page_token:
                return runs

        return runs

    def get_clusters(self, cluster_ids: Iterable[str]) -> List[Cluster]:
        clusters: List[Cluster] = []
        for cluster_id in cluster_ids:
            payload = self._get_json("/api/2.0/clusters/get", {"cluster_id": cluster_id})
            clusters.append(_parse_cluster(payload))
        return clusters

    def _get_json(self, path: str, params: Optional[Dict[str, object]] = None) -> Dict[str, object]:
        query = f"?{urlencode(params)}" if params else ""
        request = Request(
            f"{self._credentials.host}{path}{query}",
            headers={
                "Authorization": f"Bearer {self._credentials.token}",
                "Content-Type": "application/json",
            },
            method="GET",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise DatabricksClientError(
                f"Databricks API request failed with HTTP {exc.code}: {detail}"
            ) from exc
        except URLError as exc:
            raise DatabricksClientError(f"Databricks API request failed: {exc.reason}") from exc


def _parse_job(raw_job: Dict[str, object]) -> Job:
    settings = raw_job.get("settings") if isinstance(raw_job.get("settings"), dict) else {}
    raw_schedule = settings.get("schedule") if isinstance(settings, dict) else {}
    tasks = settings.get("tasks") if isinstance(settings, dict) else None
    job_clusters = settings.get("job_clusters") if isinstance(settings, dict) else None
    return Job(
        job_id=int(raw_job["job_id"]),
        name=str(settings.get("name") or raw_job.get("job_id")),
        creator_user_name=_string_or_none(raw_job.get("creator_user_name")),
        created_time=_int_or_none(raw_job.get("created_time")),
        tags=_dict_of_strings(settings.get("tags")),
        schedule=Schedule(
            quartz_cron_expression=_string_or_none(raw_schedule.get("quartz_cron_expression")),
            timezone_id=_string_or_none(raw_schedule.get("timezone_id")),
            pause_status=_string_or_none(raw_schedule.get("pause_status")),
        )
        if isinstance(raw_schedule, dict)
        else None,
        max_concurrent_runs=_int_or_none(settings.get("max_concurrent_runs")),
        tasks=[_parse_job_task(task) for task in tasks] if isinstance(tasks, list) else None,
        job_clusters=None if not isinstance(job_clusters, list) else [],
    )


def _parse_job_task(raw_task: object) -> JobTask:
    if not isinstance(raw_task, dict):
        return JobTask(task_key="unknown")

    notebook_task = raw_task.get("notebook_task")
    notebook_path = None
    if isinstance(notebook_task, dict):
        notebook_path = _string_or_none(notebook_task.get("notebook_path"))

    cluster_ref = _string_or_none(raw_task.get("job_cluster_key")) or _string_or_none(
        raw_task.get("existing_cluster_id")
    )
    return JobTask(
        task_key=str(raw_task.get("task_key") or "unknown"),
        notebook_path=notebook_path,
        cluster_ref=cluster_ref,
    )


def _parse_run(raw_run: Dict[str, object]) -> Run:
    state = raw_run.get("state")
    cluster_instance = raw_run.get("cluster_instance")
    start_time = _int_or_none(raw_run.get("start_time"))
    end_time = _int_or_none(raw_run.get("end_time"))
    duration_ms = _int_or_none(raw_run.get("run_duration")) or _int_or_none(raw_run.get("execution_duration"))
    if duration_ms is None and start_time is not None and end_time is not None:
        duration_ms = max(0, end_time - start_time)

    raw_cluster_spec = raw_run.get("cluster_spec")
    if isinstance(raw_cluster_spec, dict) and isinstance(raw_cluster_spec.get("new_cluster"), dict):
        raw_cluster_spec = raw_cluster_spec.get("new_cluster")

    return Run(
        run_id=int(raw_run["run_id"]),
        job_id=_int_or_none(raw_run.get("job_id")),
        run_name=_string_or_none(raw_run.get("run_name")),
        creator_user_name=_string_or_none(raw_run.get("creator_user_name")),
        run_page_url=_string_or_none(raw_run.get("run_page_url")),
        trigger=_string_or_none(raw_run.get("trigger")),
        start_time=start_time,
        end_time=end_time,
        duration_ms=duration_ms,
        state=RunState(
            life_cycle_state=_string_or_none(state.get("life_cycle_state")),
            result_state=_string_or_none(state.get("result_state")),
            state_message=_string_or_none(state.get("state_message")),
        )
        if isinstance(state, dict)
        else None,
        cluster_instance=ClusterInstance(
            cluster_id=_string_or_none(cluster_instance.get("cluster_id")),
            spark_context_id=_string_or_none(cluster_instance.get("spark_context_id")),
        )
        if isinstance(cluster_instance, dict)
        else None,
        cluster_spec=_parse_cluster_spec(raw_cluster_spec),
        tags=_dict_of_strings(raw_run.get("tags")),
    )


def _parse_cluster(payload: Dict[str, object]) -> Cluster:
    return Cluster(
        cluster_id=str(payload["cluster_id"]),
        cluster_name=_string_or_none(payload.get("cluster_name")),
        spark_version=_string_or_none(payload.get("spark_version")),
        node_type_id=_string_or_none(payload.get("node_type_id")),
        driver_node_type_id=_string_or_none(payload.get("driver_node_type_id")),
        num_workers=_int_or_none(payload.get("num_workers")),
        autoscale=_parse_autoscale(payload.get("autoscale")),
        autotermination_minutes=_int_or_none(payload.get("autotermination_minutes")),
    )


def _parse_cluster_spec(raw_cluster_spec: object) -> Optional[ClusterSpec]:
    if not isinstance(raw_cluster_spec, dict):
        return None

    return ClusterSpec(
        spark_version=_string_or_none(raw_cluster_spec.get("spark_version")),
        node_type_id=_string_or_none(raw_cluster_spec.get("node_type_id")),
        driver_node_type_id=_string_or_none(raw_cluster_spec.get("driver_node_type_id")),
        num_workers=_int_or_none(raw_cluster_spec.get("num_workers")),
        autoscale=_parse_autoscale(raw_cluster_spec.get("autoscale")),
        autotermination_minutes=_int_or_none(raw_cluster_spec.get("autotermination_minutes")),
    )


def _parse_autoscale(raw_autoscale: object) -> Optional[Autoscale]:
    if not isinstance(raw_autoscale, dict):
        return None

    min_workers = _int_or_none(raw_autoscale.get("min_workers"))
    max_workers = _int_or_none(raw_autoscale.get("max_workers"))
    if min_workers is None or max_workers is None:
        return None
    return Autoscale(min_workers=min_workers, max_workers=max_workers)


def _dict_of_strings(value: object) -> Optional[Dict[str, str]]:
    if not isinstance(value, dict):
        return None
    result = {str(key): str(item) for key, item in value.items()}
    return result or None


def _string_or_none(value: object) -> Optional[str]:
    return None if value is None else str(value)


def _int_or_none(value: object) -> Optional[int]:
    if value is None:
        return None
    return int(value)
