from __future__ import annotations

import json
from typing import Iterable, List, Optional, Protocol

from src.entities import (
    Autoscale,
    Cluster,
    ClusterInstance,
    ClusterSpec,
    Job,
    Run,
    RunState,
)


class StorageProtocol(Protocol):
    def save_jobs(self, jobs: Iterable[Job]) -> int: ...

    def save_runs(self, runs: Iterable[Run]) -> int: ...

    def save_clusters(self, clusters: Iterable[Cluster]) -> int: ...

    def load_jobs(self) -> List[Job]: ...

    def load_runs(self, window_start_ms: Optional[int] = None) -> List[Run]: ...

    def load_clusters(self) -> List[Cluster]: ...

    def read_metadata(self, key: str) -> Optional[str]: ...

    def write_metadata(self, key: str, value: str) -> None: ...


class Storage:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url
        self._psycopg = _import_psycopg()
        self._initialize()

    def save_jobs(self, jobs: Iterable[Job]) -> int:
        payloads = [
            (
                job.job_id,
                job.name,
                job.creator_user_name,
                job.created_time,
                json.dumps(_job_to_json(job)),
            )
            for job in jobs
        ]
        if not payloads:
            return 0

        with self._connect() as connection, connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO jobs (job_id, name, creator_user_name, created_time, payload_json)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (job_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    creator_user_name = EXCLUDED.creator_user_name,
                    created_time = EXCLUDED.created_time,
                    payload_json = EXCLUDED.payload_json,
                    synced_at = NOW()
                """,
                payloads,
            )
        return len(payloads)

    def save_runs(self, runs: Iterable[Run]) -> int:
        payloads = [
            (
                run.run_id,
                run.job_id,
                run.run_name,
                run.creator_user_name,
                run.run_page_url,
                run.trigger,
                run.start_time,
                run.end_time,
                run.duration_ms,
                run.state.life_cycle_state if run.state else None,
                run.state.result_state if run.state else None,
                run.cluster_instance.cluster_id if run.cluster_instance else None,
                json.dumps(_cluster_spec_to_json(run.cluster_spec)),
                json.dumps(run.tags or {}),
                json.dumps(_run_to_json(run)),
            )
            for run in runs
        ]
        if not payloads:
            return 0

        with self._connect() as connection, connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO runs (
                    run_id, job_id, run_name, creator_user_name, run_page_url, trigger,
                    start_time, end_time, duration_ms, life_cycle_state, result_state,
                    cluster_id, cluster_spec_json, tags_json, payload_json
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    job_id = EXCLUDED.job_id,
                    run_name = EXCLUDED.run_name,
                    creator_user_name = EXCLUDED.creator_user_name,
                    run_page_url = EXCLUDED.run_page_url,
                    trigger = EXCLUDED.trigger,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    duration_ms = EXCLUDED.duration_ms,
                    life_cycle_state = EXCLUDED.life_cycle_state,
                    result_state = EXCLUDED.result_state,
                    cluster_id = EXCLUDED.cluster_id,
                    cluster_spec_json = EXCLUDED.cluster_spec_json,
                    tags_json = EXCLUDED.tags_json,
                    payload_json = EXCLUDED.payload_json,
                    synced_at = NOW()
                """,
                payloads,
            )
        return len(payloads)

    def save_clusters(self, clusters: Iterable[Cluster]) -> int:
        payloads = [
            (
                cluster.cluster_id,
                cluster.cluster_name,
                cluster.spark_version,
                cluster.node_type_id,
                cluster.driver_node_type_id,
                cluster.num_workers,
                json.dumps(_autoscale_to_json(cluster.autoscale)),
                cluster.autotermination_minutes,
                json.dumps(_cluster_to_json(cluster)),
            )
            for cluster in clusters
        ]
        if not payloads:
            return 0

        with self._connect() as connection, connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO clusters (
                    cluster_id, cluster_name, spark_version, node_type_id, driver_node_type_id,
                    num_workers, autoscale_json, autotermination_minutes, payload_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb)
                ON CONFLICT (cluster_id) DO UPDATE SET
                    cluster_name = EXCLUDED.cluster_name,
                    spark_version = EXCLUDED.spark_version,
                    node_type_id = EXCLUDED.node_type_id,
                    driver_node_type_id = EXCLUDED.driver_node_type_id,
                    num_workers = EXCLUDED.num_workers,
                    autoscale_json = EXCLUDED.autoscale_json,
                    autotermination_minutes = EXCLUDED.autotermination_minutes,
                    payload_json = EXCLUDED.payload_json,
                    synced_at = NOW()
                """,
                payloads,
            )
        return len(payloads)

    def load_jobs(self) -> List[Job]:
        rows = self._fetch_all("SELECT payload_json FROM jobs ORDER BY name ASC")
        return [_job_from_json(_json_cell(row["payload_json"])) for row in rows]

    def load_runs(self, window_start_ms: Optional[int] = None) -> List[Run]:
        query = "SELECT payload_json FROM runs"
        params: tuple[object, ...] = ()
        if window_start_ms is not None:
            query += " WHERE start_time IS NULL OR start_time >= %s"
            params = (window_start_ms,)
        query += " ORDER BY start_time DESC"
        rows = self._fetch_all(query, params)
        return [_run_from_json(_json_cell(row["payload_json"])) for row in rows]

    def load_clusters(self) -> List[Cluster]:
        rows = self._fetch_all("SELECT payload_json FROM clusters ORDER BY cluster_id ASC")
        return [_cluster_from_json(_json_cell(row["payload_json"])) for row in rows]

    def read_metadata(self, key: str) -> Optional[str]:
        rows = self._fetch_all("SELECT value FROM metadata WHERE key = %s", (key,))
        return None if not rows else str(rows[0]["value"])

    def write_metadata(self, key: str, value: str) -> None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO metadata (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, value),
            )

    def _fetch_all(
        self, query: str, params: tuple[object, ...] = ()
    ) -> List[dict[str, object]]:
        with self._connect() as connection, connection.cursor(row_factory=self._psycopg.rows.dict_row) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def _initialize(self) -> None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    creator_user_name TEXT,
                    created_time BIGINT,
                    payload_json JSONB NOT NULL,
                    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id BIGINT PRIMARY KEY,
                    job_id BIGINT,
                    run_name TEXT,
                    creator_user_name TEXT,
                    run_page_url TEXT,
                    trigger TEXT,
                    start_time BIGINT,
                    end_time BIGINT,
                    duration_ms BIGINT,
                    life_cycle_state TEXT,
                    result_state TEXT,
                    cluster_id TEXT,
                    cluster_spec_json JSONB,
                    tags_json JSONB,
                    payload_json JSONB NOT NULL,
                    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS clusters (
                    cluster_id TEXT PRIMARY KEY,
                    cluster_name TEXT,
                    spark_version TEXT,
                    node_type_id TEXT,
                    driver_node_type_id TEXT,
                    num_workers INTEGER,
                    autoscale_json JSONB,
                    autotermination_minutes INTEGER,
                    payload_json JSONB NOT NULL,
                    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def _connect(self):
        return self._psycopg.connect(self._database_url)


class InMemoryStorage:
    def __init__(self) -> None:
        self._jobs: dict[int, Job] = {}
        self._runs: dict[int, Run] = {}
        self._clusters: dict[str, Cluster] = {}
        self._metadata: dict[str, str] = {}

    def save_jobs(self, jobs: Iterable[Job]) -> int:
        count = 0
        for job in jobs:
            self._jobs[job.job_id] = job
            count += 1
        return count

    def save_runs(self, runs: Iterable[Run]) -> int:
        count = 0
        for run in runs:
            self._runs[run.run_id] = run
            count += 1
        return count

    def save_clusters(self, clusters: Iterable[Cluster]) -> int:
        count = 0
        for cluster in clusters:
            self._clusters[cluster.cluster_id] = cluster
            count += 1
        return count

    def load_jobs(self) -> List[Job]:
        return sorted(self._jobs.values(), key=lambda job: job.name)

    def load_runs(self, window_start_ms: Optional[int] = None) -> List[Run]:
        runs = list(self._runs.values())
        if window_start_ms is not None:
            runs = [
                run
                for run in runs
                if run.start_time is None or run.start_time >= window_start_ms
            ]
        return sorted(runs, key=lambda run: run.start_time or 0, reverse=True)

    def load_clusters(self) -> List[Cluster]:
        return sorted(self._clusters.values(), key=lambda cluster: cluster.cluster_id)

    def read_metadata(self, key: str) -> Optional[str]:
        return self._metadata.get(key)

    def write_metadata(self, key: str, value: str) -> None:
        self._metadata[key] = value


def _import_psycopg():
    try:
        import psycopg  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required for Postgres storage. Install dependencies from requirements.txt."
        ) from exc
    return psycopg


def _json_cell(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    raise TypeError(f"Unsupported JSON cell type: {type(value)!r}")


def _job_to_json(job: Job) -> dict[str, object]:
    return {
        "job_id": job.job_id,
        "name": job.name,
        "creator_user_name": job.creator_user_name,
        "created_time": job.created_time,
        "tags": job.tags,
        "schedule": None
        if job.schedule is None
        else {
            "quartz_cron_expression": job.schedule.quartz_cron_expression,
            "timezone_id": job.schedule.timezone_id,
            "pause_status": job.schedule.pause_status,
        },
        "max_concurrent_runs": job.max_concurrent_runs,
        "tasks": None
        if job.tasks is None
        else [
            {
                "task_key": task.task_key,
                "notebook_path": task.notebook_path,
                "cluster_ref": task.cluster_ref,
            }
            for task in job.tasks
        ],
    }


def _job_from_json(payload: dict[str, object]) -> Job:
    from src.entities import JobTask
    from src.entities.job import Schedule

    raw_schedule = payload.get("schedule")
    raw_tasks = payload.get("tasks")
    return Job(
        job_id=int(payload["job_id"]),
        name=str(payload["name"]),
        creator_user_name=_string_or_none(payload.get("creator_user_name")),
        created_time=_int_or_none(payload.get("created_time")),
        tags=_dict_or_none(payload.get("tags")),
        schedule=Schedule(
            quartz_cron_expression=_string_or_none(raw_schedule.get("quartz_cron_expression")),
            timezone_id=_string_or_none(raw_schedule.get("timezone_id")),
            pause_status=_string_or_none(raw_schedule.get("pause_status")),
        )
        if isinstance(raw_schedule, dict)
        else None,
        max_concurrent_runs=_int_or_none(payload.get("max_concurrent_runs")),
        tasks=[
            JobTask(
                task_key=str(task["task_key"]),
                notebook_path=_string_or_none(task.get("notebook_path")),
                cluster_ref=_string_or_none(task.get("cluster_ref")),
            )
            for task in raw_tasks
        ]
        if isinstance(raw_tasks, list)
        else None,
    )


def _run_to_json(run: Run) -> dict[str, object]:
    return {
        "run_id": run.run_id,
        "job_id": run.job_id,
        "run_name": run.run_name,
        "creator_user_name": run.creator_user_name,
        "run_page_url": run.run_page_url,
        "trigger": run.trigger,
        "start_time": run.start_time,
        "end_time": run.end_time,
        "duration_ms": run.duration_ms,
        "state": None
        if run.state is None
        else {
            "life_cycle_state": run.state.life_cycle_state,
            "result_state": run.state.result_state,
            "state_message": run.state.state_message,
        },
        "cluster_instance": None
        if run.cluster_instance is None
        else {
            "cluster_id": run.cluster_instance.cluster_id,
            "spark_context_id": run.cluster_instance.spark_context_id,
        },
        "cluster_spec": _cluster_spec_to_json(run.cluster_spec),
        "tags": run.tags,
    }


def _run_from_json(payload: dict[str, object]) -> Run:
    raw_state = payload.get("state")
    raw_cluster_instance = payload.get("cluster_instance")
    return Run(
        run_id=int(payload["run_id"]),
        job_id=_int_or_none(payload.get("job_id")),
        run_name=_string_or_none(payload.get("run_name")),
        creator_user_name=_string_or_none(payload.get("creator_user_name")),
        run_page_url=_string_or_none(payload.get("run_page_url")),
        trigger=_string_or_none(payload.get("trigger")),
        start_time=_int_or_none(payload.get("start_time")),
        end_time=_int_or_none(payload.get("end_time")),
        duration_ms=_int_or_none(payload.get("duration_ms")),
        state=RunState(
            life_cycle_state=_string_or_none(raw_state.get("life_cycle_state")),
            result_state=_string_or_none(raw_state.get("result_state")),
            state_message=_string_or_none(raw_state.get("state_message")),
        )
        if isinstance(raw_state, dict)
        else None,
        cluster_instance=ClusterInstance(
            cluster_id=_string_or_none(raw_cluster_instance.get("cluster_id")),
            spark_context_id=_string_or_none(raw_cluster_instance.get("spark_context_id")),
        )
        if isinstance(raw_cluster_instance, dict)
        else None,
        cluster_spec=_cluster_spec_from_json(payload.get("cluster_spec")),
        tags=_dict_or_none(payload.get("tags")),
    )


def _cluster_to_json(cluster: Cluster) -> dict[str, object]:
    return {
        "cluster_id": cluster.cluster_id,
        "cluster_name": cluster.cluster_name,
        "spark_version": cluster.spark_version,
        "node_type_id": cluster.node_type_id,
        "driver_node_type_id": cluster.driver_node_type_id,
        "num_workers": cluster.num_workers,
        "autoscale": _autoscale_to_json(cluster.autoscale),
        "autotermination_minutes": cluster.autotermination_minutes,
    }


def _cluster_from_json(payload: dict[str, object]) -> Cluster:
    return Cluster(
        cluster_id=str(payload["cluster_id"]),
        cluster_name=_string_or_none(payload.get("cluster_name")),
        spark_version=_string_or_none(payload.get("spark_version")),
        node_type_id=_string_or_none(payload.get("node_type_id")),
        driver_node_type_id=_string_or_none(payload.get("driver_node_type_id")),
        num_workers=_int_or_none(payload.get("num_workers")),
        autoscale=_autoscale_from_json(payload.get("autoscale")),
        autotermination_minutes=_int_or_none(payload.get("autotermination_minutes")),
    )


def _cluster_spec_to_json(cluster_spec: Optional[ClusterSpec]) -> Optional[dict[str, object]]:
    if cluster_spec is None:
        return None
    return {
        "spark_version": cluster_spec.spark_version,
        "node_type_id": cluster_spec.node_type_id,
        "driver_node_type_id": cluster_spec.driver_node_type_id,
        "num_workers": cluster_spec.num_workers,
        "autoscale": _autoscale_to_json(cluster_spec.autoscale),
        "autotermination_minutes": cluster_spec.autotermination_minutes,
    }


def _cluster_spec_from_json(payload: object) -> Optional[ClusterSpec]:
    if not isinstance(payload, dict):
        return None
    return ClusterSpec(
        spark_version=_string_or_none(payload.get("spark_version")),
        node_type_id=_string_or_none(payload.get("node_type_id")),
        driver_node_type_id=_string_or_none(payload.get("driver_node_type_id")),
        num_workers=_int_or_none(payload.get("num_workers")),
        autoscale=_autoscale_from_json(payload.get("autoscale")),
        autotermination_minutes=_int_or_none(payload.get("autotermination_minutes")),
    )


def _autoscale_to_json(autoscale: Optional[Autoscale]) -> Optional[dict[str, int]]:
    if autoscale is None:
        return None
    return {"min_workers": autoscale.min_workers, "max_workers": autoscale.max_workers}


def _autoscale_from_json(payload: object) -> Optional[Autoscale]:
    if not isinstance(payload, dict):
        return None
    min_workers = _int_or_none(payload.get("min_workers"))
    max_workers = _int_or_none(payload.get("max_workers"))
    if min_workers is None or max_workers is None:
        return None
    return Autoscale(min_workers=min_workers, max_workers=max_workers)


def _string_or_none(value: object) -> Optional[str]:
    return None if value is None else str(value)


def _int_or_none(value: object) -> Optional[int]:
    if value is None:
        return None
    return int(value)


def _dict_or_none(value: object) -> Optional[dict[str, str]]:
    if not isinstance(value, dict):
        return None
    return {str(key): str(item) for key, item in value.items()}
