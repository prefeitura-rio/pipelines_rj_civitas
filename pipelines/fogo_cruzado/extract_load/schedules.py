# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

tz = pytz.timezone("America/Sao_Paulo")

fogo_cruzado_daily_clocks = [
    IntervalClock(
        interval=timedelta(hours=24),
        start_date=datetime(2024, 9, 7, 0, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

fogo_cruzado_minutely_parameters = {
    "start_date": (datetime.now(tz=tz) - timedelta(days=7)).strftime("%Y-%m-%d")
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
        cron="0-50 1-23 * * *",  # from 01:00 until 23:50
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=fogo_cruzado_minutely_parameters,
    ),
]

fogo_cruzado_etl_update_schedule = Schedule(
    clocks=untuple(fogo_cruzado_daily_clocks) + untuple(fogo_cruzado_etl_minutely_clocks)
)
