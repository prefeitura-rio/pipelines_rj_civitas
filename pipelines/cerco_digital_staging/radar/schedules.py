# -*- coding: utf-8 -*-
"""
Schedules for the "CIVITAS: cerco digital - Materialização da tabela de radares CETRIO" pipeline.
"""

from datetime import datetime

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

tz = pytz.timezone("America/Sao_Paulo")
parameters = {"select": "cerco_digital.radar"}


radar_clocks = [
    CronClock(
        cron="0 0 * * *",  # Every day at 00:00
        start_date=datetime(2026, 4, 14, 0, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=parameters,
    )
]

radar_schedule = Schedule(clocks=untuple(radar_clocks))
