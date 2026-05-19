# Air Quality Poland: Data Pipeline

A pipeline that pulls hourly air-quality readings from Polish monitoring stations, lands them as parquet, models them with dbt over DuckDB, and serves a Streamlit dashboard. I built it to get hands-on with a full Data Engineering stack: orchestration, transformations, IaC, CI.

The whole thing runs locally, no cloud account needed. The Terraform under `terraform/` plans the same warehouse on BigQuery, in case you want to move it to GCP.

## Data sources

- **GIOS** (Polish Chief Inspectorate of Environmental Protection) is the official source. About 700 stations, hourly readings for PM2.5, PM10, NO2, SO2, O3, CO, benzene.
- **OpenAQ** as a secondary source for completeness checks.

## Stack

| Layer | Tool |
|---|---|
| Ingest | Python, httpx, Prefect 3 |
| Storage (raw) | Parquet on disk |
| Warehouse | DuckDB |
| Modeling | dbt (`dbt-duckdb`) |
| Data tests | dbt tests |
| Cloud plan | Terraform for BigQuery + IAM |
| CI | GitHub Actions |
| Dashboard | Streamlit |

## Architecture

```
GIOS API \
          +--> Prefect ingest --> parquet (raw) --> dbt --> DuckDB (marts) --> Streamlit
OpenAQ  /
```

The pipeline follows a medallion layout:

- `raw`: untouched JSON from the APIs, written as parquet partitioned by ingest date
- `staging`: cleaned and typed views inside DuckDB, deduped on the natural keys
- `marts`: analytics tables, incremental on `measured_at`

dbt reads the parquet files directly via `dbt-duckdb`'s `external_location`, so there is no separate "load raw to warehouse" step.

## Layout

```
.
├── pipelines/             Prefect flows
│   └── flows/
├── dbt/                   dbt project (DuckDB)
│   └── models/
├── terraform/             GCP infra plan (BigQuery, IAM)
├── dashboard/             Streamlit app
├── tests/                 pytest
├── docs/                  ADRs
└── .github/workflows/     CI
```

## Running it locally

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

cp .env.example .env

# Pull data from the live GIOS API. Writes parquet under ./data.
python -m pipelines.flows.ingest_gios --max-stations 5

# Build the dbt models into ./air_quality.duckdb
cp dbt/profiles.yml.example $HOME/.dbt/profiles.yml
cd dbt && dbt deps && dbt run && dbt test && cd ..

pytest tests/ -v
streamlit run dashboard/app.py
```

## Status

Working:

- GIOS ingest end to end against the live API, parquet output
- Prefect flow with retries and graceful handling of the API's quirks
- dbt-duckdb staging + marts with schema tests
- GitHub Actions: lint, pytest, dbt parse, terraform validate
- Streamlit dashboard reading from DuckDB (parquet fallback if dbt has not run yet)
- Terraform plan for GCP (BigQuery datasets + service account + IAM)

To do:

- Hourly Prefect deployment with cron
- Freshness checks and Discord alerts
- Wire up a second ingest target so the BigQuery path can be exercised when GCP creds are present

## Why DuckDB

The portfolio runs without a cloud account. Anyone cloning the repo can run the whole stack in one terminal. DuckDB is what makes that work without giving up the analytical-warehouse experience: it speaks columnar storage, supports the SQL dialect dbt expects, and reads parquet at the same speeds as BigQuery for small-to-medium data. The Terraform plan under `terraform/` shows the BigQuery variant for reference.

## Notes on the GIOS API

A few rough edges that shaped how the ingest is written:

1. The keys in the JSON response are in Polish. I keep them as-is at the boundary (`STATION_MAP`, `SENSOR_MAP`, `DATA_MAP` in `ingest_gios.py`) and rename to English snake_case before anything downstream sees them.
2. Some pagination pages return HTTP 500 deterministically. Retrying does not help. The flow logs the bad page and moves on rather than dying. We lose a small slice of stations until the next run, which is fine for hourly data.
3. Sending `Accept: application/json` returns 406. The API only serves `application/ld+json`, so do not set the header.
4. Page size is capped at 100. Anything larger returns a 500 with no error body.

## Cost

$0. Everything runs locally.

## Architecture decisions

See [docs/architecture.md](docs/architecture.md) for the bigger choices.

## License

MIT. Data from GIOS is subject to its own [API terms](https://powietrze.gios.gov.pl/pjp/content/api).
