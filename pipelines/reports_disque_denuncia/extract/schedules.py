# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

disque_denuncia_etl_clocks = [
    IntervalClock(
        interval=timedelta(hours=1),
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "disque_denuncias",
            "table_id": "denuncias",
            "start_date": "2021-01-01",
            "dump_mode": "dump_mode",
            "biglake_table": "biglake_table",
        },
    )
]

disque_denuncia_etl_hour_update_schedule = Schedule(clocks=untuple(disque_denuncia_etl_clocks))
