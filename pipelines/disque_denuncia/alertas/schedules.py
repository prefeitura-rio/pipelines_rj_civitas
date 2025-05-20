# -*- coding: utf-8 -*-
"""
Schedules for the alerts pipeline.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

alertas_disque_denuncia_minutely_parameters = {
    "project_id": "rj-civitas",
    "dataset_id": "disque_denuncia",
    "table_id": "denuncias",
    "start_datetime": "2025-05-19 00:00:00",
    "keywords": ["core", "policial", "cidade de deus", "homicidio", "homicídio"],
}

alertas_disque_denuncia_minutely_clocks = [
    IntervalClock(
        interval=timedelta(minutes=1),
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=alertas_disque_denuncia_minutely_parameters,
    )
]

alertas_disque_denuncia_etl_minutely_update_schedule = Schedule(
    clocks=untuple(alertas_disque_denuncia_minutely_clocks)
)
