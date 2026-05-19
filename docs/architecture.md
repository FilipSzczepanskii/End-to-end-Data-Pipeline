# Architecture decisions

Short notes on the bigger choices in this project. The aim is to write down enough to remember why something is the way it is six months from now.

## Prefect over Airflow

I wanted an orchestrator that does not need a separate scheduler and webserver to run locally. Prefect 3 fits that. Flows and tasks are plain Python functions, which makes them trivial to unit test. The free Prefect Cloud tier (10k task runs per month) is enough to host this in a production-like setup for free.

Trade-off: Airflow still shows up in more job listings than Prefect. The concepts transfer, so I am not too worried about it.

## dbt over SQLMesh or DataForm

dbt is what the market is asking for. A quick scan of Polish Data Engineer listings on 2026-05-19 had it mentioned in roughly 80% of posts. SQLMesh is interesting but I would have to explain it in every interview.

## BigQuery over Snowflake

Two reasons. The free tier is generous enough that running this for a year costs $0. And the integration with GCP service accounts is one less moving part than connecting Snowflake to anything outside of itself.

## Medallion layout (raw / staging / marts)

Standard layering, easy to explain. The raw layer is immutable append-only with 90 day retention so I can replay transformations if I change a dbt model. Staging holds views (cheap, no storage). Marts are tables partitioned by date and clustered by station, which is what the dashboard reads.

## Graceful failure over strict ingest

The GIOS API has known sporadic 500s on some pagination pages. I chose to skip the failed pages and log them, rather than fail the whole flow. The data is hourly so anything missed gets picked up next run. The alternative (fail loudly, alert) would page me at 3am for an upstream bug I cannot fix anyway.
