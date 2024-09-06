# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

fogo_cruzado_daily_clocks = [
    IntervalClock(
        interval=timedelta(hours=24),
        start_date=datetime(2024, 9, 7, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

fogo_cruzado_etl_minutely_clocks = [
    IntervalClock(
        interval=timedelta(minutes=1),
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

fogo_cruzado_etl_daily_update_schedule = Schedule(clocks=untuple(fogo_cruzado_daily_clocks))
fogo_cruzado_etl_minutely_update_schedule = Schedule(
    clocks=untuple(fogo_cruzado_etl_minutely_clocks)
)
