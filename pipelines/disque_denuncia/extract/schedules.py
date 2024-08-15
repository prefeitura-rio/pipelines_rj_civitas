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

disque_denuncia_etl_hourly_clocks = [
    IntervalClock(
        interval=timedelta(hours=1),
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

disque_denuncia_etl_minutely_clocks = [
    IntervalClock(
        interval=timedelta(minutes=1),
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
    )
]

disque_denuncia_etl_hourly_update_schedule = Schedule(
    clocks=untuple(disque_denuncia_etl_hourly_clocks)
)
disque_denuncia_etl_minutely_update_schedule = Schedule(
    clocks=untuple(disque_denuncia_etl_minutely_clocks)
)
