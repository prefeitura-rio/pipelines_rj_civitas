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

exclude_models = ["vw_readings", "equipamento_codcet_camera_numero", "radares_cameras_proximas"]
parameters = {
    "dataset_id": "cerco_digital",
    "exclude": " ".join(exclude_models),
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
