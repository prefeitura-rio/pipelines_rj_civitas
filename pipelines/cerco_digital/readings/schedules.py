# -*- coding: utf-8 -*-
"""
Schedules for the "CIVITAS: radares_infra - Materialização das tabelas" pipeline..
"""

from datetime import datetime, timedelta, timezone

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

tz = pytz.timezone("America/Sao_Paulo")
parameters = {
    "select": "vw_readings",
    "vars": [
        {
            "start_date": (datetime.now(tz=timezone.utc) - timedelta(hours=1)).strftime(
                "%Y-%m-%d %H:00:00"
            )
        }
    ],
}


readings_clocks = [
    CronClock(
        cron="0 * * * *",  # Every hour at minute 0
        start_date=datetime(2024, 9, 7, 0, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=parameters,
    )
]

readings_schedule = Schedule(clocks=untuple(readings_clocks))
