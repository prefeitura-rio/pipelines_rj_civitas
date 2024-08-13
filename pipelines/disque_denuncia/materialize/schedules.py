# -*- coding: utf-8 -*-
"""
Schedules for the database dump_historico pipeline.
"""

from datetime import datetime

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import DateTimeClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

disque_denuncia_etl_clocks = [
    DateTimeClock(
        start_date=datetime.now(tz=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

disque_denuncia_one_time_schedule = Schedule(clocks=untuple(disque_denuncia_etl_clocks))
