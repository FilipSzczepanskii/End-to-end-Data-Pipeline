# Air Quality Poland: Data Pipeline

A pipeline that pulls hourly air-quality readings from Polish monitoring stations, loads them into BigQuery, models them with dbt, and serves a simple Streamlit dashboard. I built it to get hands-on with a full Data Engineering stack: orchestration, warehouse, transformations, IaC, CI.

## Data sources

- **GIOS** (Polish Chief Inspectorate of Environmental Protection) is the official source. About 700 stations, hourly readings for PM2.5, PM10, NO2, SO2, O3, CO, benzene.
- **OpenAQ** as a secondary source for completeness checks. Has lower-cost sensors and mobile readings the official network does not cover.

## Stack

| Layer | Tool |
|---|---|
| Ingest | Python, httpx, Prefect 3 |
| Warehouse | BigQuery |
| Modeling | dbt |
| Data tests | dbt tests, Great Expectations |
| Infra | Terraform |
| CI | GitHub Actions |
| Dashboard | Streamlit |

## Architecture

```
GIOS API \
          +--> Prefect ingest --> BigQuery raw --> dbt --> BigQuery marts --> Streamlit
OpenAQ  /
```

Ingest runs hourly. dbt builds the marts a few minutes later. The stations dimension is snapshotted daily.

The warehouse follows a medallion layout:

- `raw`: untouched JSON from the APIs, append-only, 90 day retention
- `staging`: cleaned and typed (dbt views)
- `marts`: analytics tables, partitioned by `measured_date`, clustered by `station_id`

## Layout

```
.
├── pipelines/             Prefect flows
│   ├── flows/
│   └── deployments/
├── dbt/                   dbt project
├── terraform/             GCP infra
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
# Set GCP_PROJECT_ID and OPENAQ_API_KEY (optional)

# Pull data without any cloud setup: writes parquet under ./data
python -m pipelines.flows.ingest_gios --max-stations 5

pytest tests/ -v
streamlit run dashboard/app.py
```

## Status

Already working:

- GIOS ingest, end to end, against the live API (writes parquet locally)
- Prefect flow with retries and graceful handling of the API's quirks
- dbt models defined (staging + marts) with schema tests
- Terraform plan for GCP datasets and service account
- GitHub Actions: lint, pytest, dbt parse, terraform validate

To do:

- Run `terraform apply` against a real GCP project
- BigQuery sink in the ingest flow (the parquet path stays as fallback)
- Wire up the dashboard to BigQuery
- Hourly Prefect deployment with cron
- Freshness alerts on Discord when ingest falls behind

## Notes on the GIOS API

The live API has a few rough edges that I had to handle. I am noting them because they shaped how the ingest is written:

1. The keys in the JSON response are in Polish. I keep them as-is at the boundary (`STATION_MAP`, `SENSOR_MAP`, `DATA_MAP` in `ingest_gios.py`) and rename to English snake_case before anything downstream sees them.
2. Some pagination pages return HTTP 500 deterministically. Retrying does not help. The flow logs the bad page and moves on instead of dying. We lose a small slice of stations until the next run, which is fine for hourly data.
3. Sending `Accept: application/json` returns 406. The API only serves `application/ld+json`, so do not set the header.
4. Page size is capped at 100. Anything larger gets a 500 with no error body.

## Cost

The GCP free tier covers all of this. BigQuery gives 1 TB of queries and 10 GB of storage per month for free, and this project uses a tiny fraction of both. Prefect runs locally or on the free Cloud tier.

## Architecture decisions

See [docs/architecture.md](docs/architecture.md) for why I picked Prefect over Airflow, BigQuery over Snowflake, and so on.

## License

MIT. Data from GIOS is subject to its own [API terms](https://powietrze.gios.gov.pl/pjp/content/api).
