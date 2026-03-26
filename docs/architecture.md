# Facebrick Architecture

## Overview

This repository now includes a local `front-back-db` implementation for the
FinOps module:

* `frontend/` is a browser dashboard that only talks to backend API endpoints.
* `src/app/` is the FastAPI backend, Databricks client, Postgres
  persistence, and FinOps API layer.
* `docker-compose.yml` runs dedicated `frontend`, `backend`, and `db`
  containers.

## Runtime Flow

1. The backend reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from its own
   environment.
2. `POST /api/finops/sync` fetches Jobs, Runs, and Cluster metadata from
   Databricks.
3. The backend stores synchronized snapshots in Postgres.
4. The FinOps service applies the local pricing map and computes dashboard
   aggregates.
5. Nginx serves the frontend and proxies `/api/*` traffic to the backend.

## Security Model

* The Databricks token stays only in the backend via environment variables.
* The frontend never reads the Databricks token directly.
* The backend binds to `127.0.0.1` by default outside containers.
* In the containerized setup, browser clients reach the backend through an
  internal Docker network and Nginx proxy, so the Databricks token stays
  server-side.
* Cross-origin API access is disabled by default. If you deploy the frontend
  and backend on separate origins, set `FACEBRICK_ALLOWED_ORIGINS`.
* If you self-host this on the internet, enable
  `FACEBRICK_BASIC_AUTH_USER`/`FACEBRICK_BASIC_AUTH_PASSWORD` and put it behind
  TLS or a reverse proxy.

## Environment Variables

Copy `.env.example` into your preferred shell or dotenv workflow and set:

* `DATABRICKS_HOST`
* `DATABRICKS_TOKEN`
* `FACEBRICK_DATABASE_URL`
* `FACEBRICK_PRICING_FILE`

Optional:

* `FACEBRICK_BIND`
* `FACEBRICK_PORT`
* `FACEBRICK_ALLOWED_ORIGINS`
* `FACEBRICK_BASIC_AUTH_USER`
* `FACEBRICK_BASIC_AUTH_PASSWORD`

## Run Locally

```bash
docker compose up --build
```

Open `http://127.0.0.1:8080`, then click `Sync Databricks`.

For a non-container backend run:

```bash
pip install -r requirements.txt
python3 main.py
```

## FinOps API Endpoints

* `GET /api/health`
* `POST /api/finops/sync`
* `GET /api/finops/dashboard?window_days=30`
* `GET /api/finops/summary?window_days=30`
* `GET /api/finops/jobs?window_days=30`
* `GET /api/finops/runs?window_days=30&limit=20`
* `GET /api/finops/insights?window_days=30`
