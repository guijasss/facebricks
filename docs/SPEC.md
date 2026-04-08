# Project Spec: Facebrick v1 Foundation Layer

## 1. Context
- **Product**: A Databricks intelligence layer that converts operational metadata into actionable insights for cost visibility, job observability, data freshness, and proactive alerts.
- **Users**: Data platform engineers, data engineers, FinOps practitioners, analytics platform owners, and engineering managers responsible for Databricks operations.
- **Problem being solved**: Databricks operational signals such as jobs, runs, cluster usage, and table health are fragmented and hard to translate into action. Teams need one place to understand where money is being spent, what is failing, and which datasets are stale or broken.
- **Existing system**: Databricks exposes raw operational data, but the product vision in `README.md` identifies a gap between raw signals and decision-ready intelligence. The current repository already contains a local FinOps implementation with a frontend, FastAPI backend, Databricks sync flow, and Postgres persistence, but the broader v1 product scope still needs to be formalized.

## 2. Functional Requirements

### Core v1 scope
- The system must ingest Databricks operational metadata required for v1 analysis, including jobs, runs, cluster metadata, and any available table health signals.
- The system must persist synchronized snapshots so users can analyze trends over time instead of only current state.
- The system must provide a UI and API that expose actionable summaries instead of raw Databricks responses.

### FinOps
- The system must show cost by job.
- The system should show cost by pipeline when the source metadata is available.
- The system should show cost by table when lineage or ownership data is available enough to attribute it.
- The system must show cost over time for a configurable analysis window.
- The system must identify the most expensive jobs and the most expensive maintained datasets.
- The system must produce insight statements such as "this job represents X% of total cost" and "this table costs X per day to maintain."

### Job Observability
- The system must track runtime history for jobs.
- The system must calculate job failure rate.
- The system should identify orphan jobs that appear to have no active owner or operational stewardship.
- The system must report execution frequency.
- The system should derive instability indicators such as high runtime variance.
- The system should identify performance degradation trends over time.
- The system must produce insight statements such as "this job is getting slower" and "this job fails above the fleet average."

### Data Quality and Freshness
- The system should monitor table freshness against a defined SLA when freshness metadata is available.
- The system should detect meaningful changes in data volume over time.
- The system should detect schema changes that may indicate breaking or unexpected upstream behavior.
- The system should classify issues into stale data, inconsistent data, and volume anomalies.
- The system should connect freshness and quality issues to upstream dependencies when that metadata is available.
- The system must produce insight statements such as "this table missed its SLA" and "volume dropped 80% below expected."

### Alerts
- The system must support threshold-based alerts for conditions such as unusually high cost, repeated failures, or missed freshness SLA.
- The system should support anomaly-based alerts for behavior outside a learned or baseline pattern.
- The alerting model must favor proactive notification over passive dashboards.

### API and Dashboard
- The system must expose backend endpoints for health checks, synchronization, dashboard views, summaries, job views, run views, and insights.
- The system must provide a browser-accessible dashboard that reads only from backend APIs.
- The system should allow users to filter analysis by a time window, such as the current `window_days` pattern used by the API.

## 3. Success Criteria
- [ ] Users can answer the three v1 product questions: where money is being spent, what is failing, and which data assets are broken or late.
- [ ] A sync operation imports Databricks metadata into local persistence without exposing Databricks credentials to the frontend.
- [ ] The dashboard and API expose cost summaries, top cost drivers, recent runs, and generated insights for a configurable time window.
- [ ] The system identifies at least one actionable insight in each enabled domain when supporting data exists: cost, job reliability, and data freshness.
- [ ] Threshold alerts can be configured and triggered for at least high cost, high failure rate, and missed freshness SLA conditions.
- [ ] Unit tests cover the core analysis logic for pricing, aggregation, and insight generation.

## 4. Test Cases
| ID  | Scenario | Input | Expected Output | Edge case? |
|-----|----------|-------|-----------------|------------|
| T01 | FinOps sync happy path | Valid Databricks host, token, pricing file, and reachable API | Sync completes, snapshots persist, dashboard reflects current imported data | No |
| T02 | Cost aggregation by job | Multiple runs across jobs within `window_days=30` | Summary returns total cost, per-job cost, and ranking of top cost drivers | No |
| T03 | Runtime degradation detection | Historical runs with rising average duration for one job | Insights endpoint flags slowdown trend for that job | No |
| T04 | High failure rate detection | Job history with repeated failures above baseline | Insights or alerts classify the job as unreliable | No |
| T05 | Freshness SLA missed | Table metadata shows last update beyond configured SLA | System marks table as stale and emits freshness insight or alert | No |
| T06 | Volume anomaly | Table volume drops sharply from historical pattern | System flags anomaly and includes expected versus actual context | No |
| T07 | Missing optional data | Sync returns jobs and runs but no lineage or table health metadata | FinOps and job observability still work, unavailable sections degrade gracefully | Yes |
| T08 | Invalid credentials | Missing or invalid `DATABRICKS_TOKEN` | Sync fails safely, returns an error, and does not leak secrets to the client | Yes |
| T09 | Empty workspace window | Valid sync but no runs in selected analysis window | Dashboard returns zero-state summaries without crashing | Yes |
| T10 | Threshold alert boundary | Metric equals alert threshold exactly | Alert behavior matches the defined threshold rule consistently | Yes |

## 5. Technical Constraints
- **Stack**: Python backend with FastAPI, Postgres for persistence, browser frontend served by Nginx, Docker Compose for local orchestration.
- **Integrations**: Databricks REST APIs and metadata endpoints, local pricing configuration file, optional future metadata sources for table freshness and lineage.
- **Performance**: Sync should support routine manual refresh for local operational analysis; dashboard endpoints should return interactive summaries for common windows such as 7 to 30 days.
- **Security**: Databricks credentials must remain backend-only via environment variables. Frontend access must occur through backend APIs. Cross-origin access is disabled by default, and internet-facing deployment should use basic auth plus TLS or a reverse proxy.
- **Deployment**: Local containerized deployment via `docker compose up --build`; non-container backend execution via `python3 main.py`; configuration through environment variables such as `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `FACEBRICK_DATABASE_URL`, and `FACEBRICK_PRICING_FILE`.
- **Data availability**: Some v1 capabilities depend on metadata that may not be directly available from Databricks without additional collectors or conventions, especially table-level cost attribution, freshness SLA definitions, and upstream dependency context.

## 6. Non-Goals (Out of Scope)
- Expanded lineage graph navigation across job, table, pipeline, and notebook relationships is out of scope for v1 and belongs to v2.
- Automatic optimization recommendations based on Spark event logs, query plans, skew detection, spill analysis, or cluster right-sizing are out of scope for v1 and belong to v2.
- Automatic remediation or autonomous optimization actions are out of scope for v1.
- Full root-cause analysis across downstream impact chains is out of scope for v1.
- Passive replication of all raw Databricks telemetry without derived insight is not the goal; v1 focuses on actionable summaries.

## 7. Open Questions
- What metadata source will define table freshness SLA, expected volume, and schema baselines?
- How should cost be attributed to tables and pipelines when multiple jobs contribute to the same downstream asset?
- What rule defines an orphan job in this product: missing owner tag, disabled schedule, no recent runs, or another ownership heuristic?
- Which alert delivery channels are required for v1: in-dashboard only, email, Slack, webhook, or another mechanism?
- What level of anomaly detection is expected in v1 versus simple statistical thresholds?
- Should v1 focus on batch jobs first, or must it also cover Delta Live Tables, pipelines, and SQL workloads from the beginning?
