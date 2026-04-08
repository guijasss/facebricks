# Run Instructions

## Local backend

Requirements:

- Python 3.12
- PostgreSQL running locally on `127.0.0.1:5432`

Install dependencies:

```bash
pip install -r requirements.txt
```

Set environment variables as needed:

```bash
cp config/facebrick.config.example.json config/facebrick.config.json
```

Optional variables:

```bash
export FACEBRICK_ALLOWED_ORIGINS="http://127.0.0.1:8080,http://localhost:8080"
export FACEBRICK_BASIC_AUTH_USER="<user>"
export FACEBRICK_BASIC_AUTH_PASSWORD="<password>"
```

Edit `config/facebrick.config.json` to set:

- `database_url`
- `databricks.host`
- `databricks.token`
- `pricing.currency`
- `pricing.rates`

Start the app from the repository root:

```bash
python main.py
```

The backend listens on `http://127.0.0.1:8000` by default.

## Docker Compose

Start the full stack from the repository root:

```bash
docker compose up --build
```

Services:

- Frontend: `http://localhost:8080`
- Backend: internal Compose service on port `8000`
- PostgreSQL: internal Compose service `db:5432`

The backend container reads its runtime settings from:

```bash
config/facebrick.config.json
```

The Compose backend mounts the repository into `/app` and runs with `uvicorn --reload`, so edits to backend code and files under `config/` are visible without rebuilding or restarting the container.

## Notes

- If `databricks.host` and `databricks.token` are empty in [config/facebrick.config.json](/home/condo-142/dev/facebricks/config/facebrick.config.json), the app still starts, but sync endpoints reject Databricks operations.
- Use [config/facebrick.config.example.json](/home/condo-142/dev/facebricks/config/facebrick.config.example.json) as the template for new environments.
