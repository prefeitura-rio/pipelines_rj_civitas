# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

tz = pytz.timezone("America/Sao_Paulo")

fogo_cruzado_daily_parameters = {
    "bigquery_project": "civitas",
    "github_repo": "https://github.com/prefeitura-rio/pipelines_rj_civitas",
    "send_discord_report": True,
    "start_date": (datetime.now(tz=tz) - timedelta(days=30)).strftime("%Y-%m-%d"),
    "prefix": "PARTIAL_REFRESH_",
    "write_disposition": "WRITE_APPEND",
}


fogo_cruzado_daily_clocks = [
    CronClock(
        cron="0 0 * * *",  # Todo dia às 00:00
        start_date=datetime(2024, 9, 7, 0, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=fogo_cruzado_daily_parameters,
    )
]

fogo_cruzado_minutely_parameters = {
    "bigquery_project": "civitas",
    "dbt_secrets": ["DISCORD_WEBHOOK_URL_DBT-RUNS"],
    "github_repo": "https://github.com/prefeitura-rio/pipelines_rj_civitas",
    "send_discord_report": True,
    "start_date": datetime.now(tz=tz).strftime("%Y-%m-%d"),
    "prefix": "PARTIAL_REFRESH_",
    "write_disposition": "WRITE_APPEND",
}
fogo_cruzado_etl_minutely_clocks = [
    CronClock(
        cron="10-59 0 * * *",  # from 00:10 until 00:59
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=fogo_cruzado_minutely_parameters,
    ),
    CronClock(
        cron="* 1-22 * * *",  # from 01:00 until 22:59
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=fogo_cruzado_minutely_parameters,
    ),
    CronClock(
        cron="0-50 23 * * *",  # from 23:00 until 23:50
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=fogo_cruzado_minutely_parameters,
    ),
]

fogo_cruzado_etl_update_schedule = Schedule(
    # clocks=untuple(fogo_cruzado_daily_clocks) + untuple(fogo_cruzado_etl_minutely_clocks)
    clocks=untuple(fogo_cruzado_daily_clocks)
)
