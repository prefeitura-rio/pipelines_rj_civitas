# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

tz = pytz.timezone("America/Sao_Paulo")


parameters = {
    "dataset_id": "cerco_digital",
    "table_id": "cameras"
}
daily_schedule = [
    IntervalClock(
        interval=timedelta(hours=24),
        start_date=datetime(2024, 9, 7, 0, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=parameters
    )
]

cerco_digital_cameras_schedule = Schedule(
    clocks=daily_schedule
)
