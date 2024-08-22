# -*- coding: utf-8 -*-
"""
Schedules for the "CIVITAS: radares_infra - Materialização das tabelas" pipeline.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

radares_infra_twice_daily_clocks = [
    IntervalClock(
        interval=timedelta(hours=12),
        start_date=datetime(2024, 8, 23, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

radares_infra_twice_daily_update_schedule = Schedule(
    clocks=untuple(radares_infra_twice_daily_clocks)
)
