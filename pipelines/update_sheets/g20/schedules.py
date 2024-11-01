# -*- coding: utf-8 -*-
"""
Schedules for the spreadsheet update
"""

from datetime import datetime

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

g20_sheets_etl_clocks = [
    CronClock(
        cron="55 8,13,18 * * *",  # 08h55, 13h55 and 18h55
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

g20_etl_update_schedule = Schedule(clocks=untuple(g20_sheets_etl_clocks))
