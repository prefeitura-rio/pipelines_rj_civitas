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

tz = pytz.timezone("America/Sao_Paulo")


fogo_cruzado_minutely_parameters = {
    "project_id": "rj-civitas",
    "dataset_id": "scraping_redes",
    "dataset_id_staging": "scraping_redes_staging",
    "table_id_usuarios": "usuarios_monitorados",
    "table_id_messages": "telegram_messages",
    "table_id_chats": "telegram_chats",
    "write_disposition_chats": "WRITE_APPEND",
    "write_disposition_messages": "WRITE_APPEND",
    "start_date": "2024-11-11 00:00:00",
    "end_date": "2024-11-13 03:00:00",
    "mode": "staging",
}

telegram_interval_clocks = [
    IntervalClock(
        interval=timedelta(minutes=1),
        start_date=datetime(2024, 9, 7, 0, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=fogo_cruzado_minutely_parameters,
    ),
]

telegram_update_schedule = Schedule(clocks=untuple(telegram_interval_clocks))
