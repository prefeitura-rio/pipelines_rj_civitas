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

integracao_reports_daily_clocks = [
    IntervalClock(
        interval=timedelta(hours=24),
        start_date=datetime(2024, 8, 30, 8, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

integracao_reports_1746_daily_update_schedule = Schedule(
    clocks=untuple(integracao_reports_daily_clocks)
)
