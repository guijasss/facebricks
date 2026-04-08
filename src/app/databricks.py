from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.entities import (
    Autoscale,
    Cluster,
    ClusterInstance,
    ClusterSpec,
    Job,
    JobClusterSpec,
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

    def list_distinct_cluster_node_types(self, warehouse_id: str) -> List[str]:
        payload = self._post_json(
            "/api/2.0/sql/statements/",
            {
                "warehouse_id": warehouse_id,
                "statement": (
                    "SELECT DISTINCT node_type "
                    "FROM ("
                    "  SELECT driver_node_type AS node_type "
                    "  FROM system.compute.clusters "
                    "  WHERE driver_node_type IS NOT NULL "
                    "  UNION "
                    "  SELECT worker_node_type AS node_type "
                    "  FROM system.compute.clusters "
                    "  WHERE worker_node_type IS NOT NULL"
                    ") cluster_nodes "
                    "ORDER BY node_type"
                ),
                "wait_timeout": "30s",
            },
        )
        statement_id = _string_or_none(payload.get("statement_id"))
        if statement_id is None:
            raise DatabricksClientError(
                "Databricks SQL statement execution did not return a statement_id."
            )

        status = payload.get("status")
        state = _statement_state(status)
        while state in {"PENDING", "RUNNING"}:
            time.sleep(0.5)
            payload = self._get_json(f"/api/2.0/sql/statements/{statement_id}")
            state = _statement_state(payload.get("status"))

        if state != "SUCCEEDED":
            raise DatabricksClientError(
                f"Databricks SQL statement failed with state '{state or 'UNKNOWN'}'."
            )

        rows = _statement_rows(payload)
        node_types = sorted(
            {
                str(row.get("node_type")).strip()
                for row in rows
                if row.get("node_type") is not None and str(row.get("node_type")).strip()
            }
        )
        return node_types

    def _get_json(self, path: str, params: Optional[Dict[str, object]] = None) -> Dict[str, object]:
        return self._request_json("GET", path, params=params)

    def _post_json(self, path: str, payload: Dict[str, object]) -> Dict[str, object]:
        return self._request_json("POST", path, body=payload)

    def _request_json(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, object]] = None,
        body: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        query = f"?{urlencode(params)}" if params else ""
        request = Request(
            f"{self._credentials.host}{path}{query}",
            data=json.dumps(body).encode("utf-8") if body is not None else None,
            headers={
                "Authorization": f"Bearer {self._credentials.token}",
                "Content-Type": "application/json",
            },
            method=method,
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
    parsed_job_clusters: Optional[List[JobClusterSpec]] = None
    if isinstance(job_clusters, list):
        parsed_job_clusters = []
        for job_cluster in job_clusters:
            parsed_job_cluster = _parse_job_cluster_spec(job_cluster)
            if parsed_job_cluster is not None:
                parsed_job_clusters.append(parsed_job_cluster)

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
        job_clusters=parsed_job_clusters,
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


def _parse_job_cluster_spec(raw_job_cluster: object) -> Optional[JobClusterSpec]:
    if not isinstance(raw_job_cluster, dict):
        return None

    new_cluster = _parse_cluster_spec(raw_job_cluster.get("new_cluster"))
    job_cluster_key = _string_or_none(raw_job_cluster.get("job_cluster_key"))
    if job_cluster_key is None or new_cluster is None:
        return None

    return JobClusterSpec(job_cluster_key=job_cluster_key, new_cluster=new_cluster)


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


def _statement_state(value: object) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    return _string_or_none(value.get("state"))


def _statement_rows(payload: Dict[str, object]) -> List[Dict[str, Any]]:
    result = payload.get("result")
    manifest = payload.get("manifest")
    if not isinstance(result, dict) or not isinstance(manifest, dict):
        return []

    raw_rows = result.get("data_array")
    schema = manifest.get("schema")
    if not isinstance(raw_rows, list) or not isinstance(schema, dict):
        return []

    raw_columns = schema.get("columns")
    if not isinstance(raw_columns, list):
        return []

    columns = [
        str(column.get("name"))
        for column in raw_columns
        if isinstance(column, dict) and column.get("name") is not None
    ]
    rows: List[Dict[str, Any]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, list):
            continue
        row: Dict[str, Any] = {}
        for index, column_name in enumerate(columns):
            row[column_name] = raw_row[index] if index < len(raw_row) else None
        rows.append(row)
    return rows
