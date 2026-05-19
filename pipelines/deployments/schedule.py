"""
Prefect deployment definitions. Run after `prefect server start` and worker creation.

    python -m pipelines.deployments.schedule

Schedules:
- ingest-gios:    every hour at :05
- ingest-openaq:  every 3 hours
"""

from prefect import serve
from prefect.schedules import Cron

from pipelines.flows.ingest_gios import ingest_gios
from pipelines.flows.ingest_openaq import ingest_openaq

if __name__ == "__main__":
    gios_dep = ingest_gios.to_deployment(
        name="hourly-gios",
        schedule=Cron("5 * * * *", timezone="Europe/Warsaw"),
        parameters={"local": False},
        tags=["ingest", "gios"],
    )

    openaq_dep = ingest_openaq.to_deployment(
        name="3h-openaq",
        schedule=Cron("10 */3 * * *", timezone="Europe/Warsaw"),
        parameters={"local": False},
        tags=["ingest", "openaq"],
    )

    serve(gios_dep, openaq_dep)
