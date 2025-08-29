# -*- coding: utf-8 -*-
"""
Schedules for the "CIVITAS: radares_infra - Materialização das tabelas" pipeline..
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants


parameters = {
    "RENAME_FLOW": True,
    "SEND_DISCORD_REPORT": True,
    "COMMAND": "build",
    "SELECT": "vw_readings",
    "GITHUB_REPO": "https://github.com/prefeitura-rio/pipelines_rj_civitas",
    "BIGQUERY_PROJECT": "civitas",
    "DBT_SECRETS": ["DISCORD_WEBHOOK_URL_DBT-RUNS"],
    "ENVIRONMENT": "dev",
    "SECRETS_PATH": "/discord"
}

readings_clocks = [
    IntervalClock(
        interval=timedelta(hours=1),
        start_date=datetime(2024, 8, 23, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=parameters,
    )
]

readings_schedule = Schedule(
    clocks=untuple(readings_clocks)
)
