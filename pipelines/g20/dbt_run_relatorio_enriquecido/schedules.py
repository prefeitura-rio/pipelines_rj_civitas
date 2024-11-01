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
# G20 AI Reports
#
#####################################

parameters_tables = [
    {
        "dataset_id": "g20",
        "table_id": "reports_enriquecidos_filtrados",
    },
]

g20_report_clocks = [
    IntervalClock(
        interval=timedelta(minutes=10),
        start_date=datetime(2024, 1, 1, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "g20",
            "table_id": parameters["table_id"],
            # "upstream": parameters["upstream"],
        },
    )
    for count, parameters in enumerate(parameters_tables)
]
g20_reports_schedule = Schedule(clocks=untuple(g20_report_clocks))
