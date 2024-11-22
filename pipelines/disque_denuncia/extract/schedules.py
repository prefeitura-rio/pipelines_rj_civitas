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

disque_denuncia_minutely_parameters = {
    "project_id": "rj-civitas",
    "dataset_id": "disque_denuncia",
    "table_id": "denuncias",
    "start_date": "2021-01-01",
    "tipo_difusao": "interesse",
    "loop_limiter": False,
    "dump_mode": "append",
    "biglake_table": True,
    "mod": 100,
    "materialize_after_dump": True,
    "dbt_alias": False,
    "materialize_reports_dd_after_dump": True,
    "georeference_reports": True,
    "mode": "prod",
    "id_column_name": "id_denuncia",
    "address_columns": [
        "tipo_logradouro",
        "logradouro",
        "numero_logradouro",
        "bairro_logradouro",
        "cep_logradouro",
        "municipio",
        "estado",
    ],
    "lat_lon_columns": {"latitude": "latitude", "longitude": "longitude"},
    "timestamp_creation_column_name": "timestamp_insercao",
    "date_column_name_geocoding": "data_denuncia",
}

disque_denuncia_etl_minutely_clocks = [
    IntervalClock(
        interval=timedelta(minutes=1),
        start_date=datetime(2021, 11, 23, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=disque_denuncia_minutely_parameters,
    )
]

disque_denuncia_etl_minutely_update_schedule = Schedule(
    clocks=untuple(disque_denuncia_etl_minutely_clocks)
)
