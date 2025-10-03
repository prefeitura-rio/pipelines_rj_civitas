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

params = {"dlq_names": ["ocr", "monitor-plates"]}
alertas_data_relay_dlq_monitor_5min_clocks = [
    IntervalClock(
        interval=timedelta(minutes=5),
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=params,
    )
]

alertas_data_relay_dlq_monitor_5min_update_schedule = Schedule(
    clocks=untuple(alertas_data_relay_dlq_monitor_5min_clocks)
)
