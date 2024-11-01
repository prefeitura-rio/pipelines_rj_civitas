# -*- coding: utf-8 -*-
"""
Append new data to google sheets
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.update_sheets.g20.schedules import g20_etl_update_schedule
from pipelines.update_sheets.g20.tasks import (
    task_append_new_data_to_google_sheets,
    task_get_disque_denuncia_newest_data,
    task_get_fogo_cruzado_newest_data,
    task_get_secret_folder,
    task_get_wave_police_newest_data,
)

with Flow(
    name="CIVITAS: G20 - ATUALIZAR SHEETS",
    state_handlers=[handler_inject_bd_credentials, handler_initialize_sentry],
) as update_g20_sheets:
    spreadsheet_id = Parameter(
        "spreadsheet_id", default="1r2fJAVlg5AbfuyYH3bIKycHOsoj80ZBrXhL7QWqOLag"
    )
    range_name_read = Parameter(
        "range_name_read",
        default={
            "disque_denuncia": "Disque Denúncia!A2:B",
            "fogo_cruzado": "Fogo Cruzado!A2:A",
            "waze_police": "Waze - Policiamento!A2:A",
        },
    )
    range_name_write = Parameter(
        "range_name_write",
        default={
            "disque_denuncia": "Disque Denúncia!A1",
            "fogo_cruzado": "Fogo Cruzado!A1",
            "waze_police": "Waze - Policiamento!A1",
        },
    )

    api_civitas_secrets = task_get_secret_folder(secret_path="/api-civitas")

    dd = task_get_disque_denuncia_newest_data(
        spreadsheet_id=spreadsheet_id, range_name=range_name_read["disque_denuncia"]
    )

    fc = task_get_fogo_cruzado_newest_data(
        spreadsheet_id=spreadsheet_id, range_name=range_name_read["fogo_cruzado"]
    )

    police = task_get_wave_police_newest_data(
        api_secrets=api_civitas_secrets,
        spreadsheet_id=spreadsheet_id,
        range_name=range_name_read["waze_police"],
    )

    dd_update = task_append_new_data_to_google_sheets(
        values=dd, spreadsheet_id=spreadsheet_id, range_name=range_name_write["disque_denuncia"]
    )
    dd_update.set_upstream(dd)

    fc_update = task_append_new_data_to_google_sheets(
        values=fc, spreadsheet_id=spreadsheet_id, range_name=range_name_write["fogo_cruzado"]
    )
    fc_update.set_upstream(fc)

    police_update = task_append_new_data_to_google_sheets(
        values=police, spreadsheet_id=spreadsheet_id, range_name=range_name_write["waze_police"]
    )
    police_update.set_upstream(police)

update_g20_sheets.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
update_g20_sheets.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

update_g20_sheets.schedule = g20_etl_update_schedule
