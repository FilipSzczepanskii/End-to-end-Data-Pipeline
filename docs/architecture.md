# Architecture decisions

Short notes on the bigger choices in this project.

## Prefect over Airflow

I wanted an orchestrator that does not need a separate scheduler and webserver to run locally. Prefect 3 fits that. Flows and tasks are plain Python functions, which makes them trivial to unit test. The free Prefect Cloud tier (10k task runs per month) is enough to host this in a production-like setup for free.

Trade-off: Airflow shows up in more job listings than Prefect. The concepts transfer, so I am not too worried about it.

## dbt over SQLMesh or DataForm

dbt is what the market is asking for. A quick scan of Polish Data Engineer listings on 2026-05-19 had it mentioned in roughly 80% of posts.

## DuckDB over BigQuery (for the default target)

I wanted the whole project to run with `git clone && pip install && run`. That rules out anything that needs a cloud account, a card on file, or a multi-step auth flow. DuckDB ticks all the boxes: real columnar storage, full SQL, parquet reads at warehouse-comparable speeds for this data size, and dbt has a first-party adapter.

The BigQuery plan still lives under `terraform/` because the migration path matters. The dbt project only needs the profile swapped to switch warehouses. Picking DuckDB also lands me in a stack that has been getting heavy use in real analytics teams (MotherDuck, dbt Labs, Fivetran), so it is not a step down for the portfolio.

## Medallion layout (raw / staging / marts)

Standard layering, easy to explain. Raw is immutable parquet on disk, partitioned by ingest date. Staging is DuckDB views (cheap, no storage). Marts are tables, with `fct_measurements_hourly` built incrementally on `measured_at`.

The raw layer being parquet, not a warehouse table, is deliberate. It keeps the warehouse layer empty until dbt builds it, which means I can drop and rebuild DuckDB whenever I want without losing data.

## Graceful failure over strict ingest

The GIOS API has known sporadic 500s on some pagination pages. I chose to skip the failed pages and log them, rather than fail the whole flow. The data is hourly so anything missed gets picked up next run. The alternative (fail loudly, alert) would page me at 3am for an upstream bug I cannot fix anyway.
