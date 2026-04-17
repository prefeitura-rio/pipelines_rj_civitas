# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline.
"""

from datetime import datetime, timedelta, timezone

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

parameters = {
    "dataset_id": "cerco_digital",
    "exclude": "vw_readings radar equipamento_codcet_camera_numero",
    "vars": [
        {
            "start_date": (datetime.now(tz=timezone.utc) - timedelta(hours=1)).strftime(
                "%Y-%m-%d %H:00:00"
            )
        }
    ]
}

auxiliary_tables_daily_clocks = [
    IntervalClock(
        interval=timedelta(hours=24),
        start_date=datetime(2024, 8, 30, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=parameters,
    )
]

auxiliary_tables_daily_update_schedule = Schedule(clocks=untuple(auxiliary_tables_daily_clocks))
