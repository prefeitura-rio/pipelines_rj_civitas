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

#####################################
#
# CIVITAS Disque Denúncia Datalake Schedules
#
#####################################

civitas_disque_denuncia_datalake_tables = {
    "stg_denuncias": "stg_denuncias",
    "assuntos_classes": "assuntos_classes",
    "assuntos_tipos": "assuntos_tipos",
    "denuncias_assuntos": "denuncias_assuntos",
    "denuncias_orgaos": "denuncias_orgaos",
    "denuncias_xptos": "denuncias_xptos",
    "envolvidos": "envolvidos",
    "orgaos": "orgaos",
    "xptos": "xptos",
    "denuncias": "denuncias",
}

civitas_disque_denuncia_datalake_clocks = [
    IntervalClock(
        interval=timedelta(hours=1),
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "disque_denuncia",
            "table_id": table_id,
            "mode": "prod",
        },
    )
    for table_id in civitas_disque_denuncia_datalake_tables.values()
]
civitas_disque_denuncia_datalake_hourly_update_schedule = Schedule(
    clocks=untuple(civitas_disque_denuncia_datalake_clocks)
)
