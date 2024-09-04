# -*- coding: utf-8 -*-
"""
Schedules for the "CIVITAS: Placas Clonadas - Materialização das tabelas" pipeline.
"""

from datetime import datetime

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

placas_clonadas_monthly_clock = [
    CronClock(
        cron='0 0 1 * *',
        start_date=datetime(2024, 9, 4, 11, 5, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

placas_clonadas_monthly_update_schedule = Schedule(
    clocks=untuple(placas_clonadas_monthly_clock)
)
