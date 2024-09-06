# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data.
related to 'Disque Denúncia' reports.
"""
from datetime import datetime, timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_get_current_flow_run_labels,
    task_get_flow_group_id,
)
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants
from pipelines.fogo_cruzado.extract_load.schedules import (
    fogo_cruzado_etl_daily_update_schedule,
    fogo_cruzado_etl_minutely_update_schedule,
)
from pipelines.fogo_cruzado.extract_load.tasks import (
    check_report_qty,
    fetch_occurrences,
    load_to_table,
    task_get_secret_folder,
)

# from os import environ


# from pipelines.fogo_cruzado.extract_load.utils import (
#     handler_inject_fogocruzado_credentials,
# )

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="[TEMPLATE]: Fogo Cruzado - Extração e Carga",
    state_handlers=[
        handler_inject_bd_credentials,
        # handler_inject_fogocruzado_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as template_extracao_fogo_cruzado:

    start_date = Parameter("start_date", default=None)
    project_id = Parameter("project_id", default="rj-civitas")
    dataset_id = Parameter("dataset_id", default="fogo_cruzado_staging")
    table_id = Parameter("table_id", default="ocorrencias")
    materialize_after_dump = Parameter("materialize_after_dump", default=False)
    secrets = task_get_secret_folder(secret_path="/api-fogo-cruzado")

    # Task to get reports from the specified start date
    occurrences_reponse = fetch_occurrences(
        email=secrets["FOGOCRUZADO_USERNAME"],
        password=secrets["FOGOCRUZADO_PASSWORD"],
        initial_date=start_date,
    )

    # Task to check report quantity
    report_qty_check = check_report_qty(task_response=occurrences_reponse)
    report_qty_check.set_upstream(occurrences_reponse)

    load_to_table_response = load_to_table(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        occurrences=occurrences_reponse[0],
    )

    load_to_table_response.set_upstream(report_qty_check)

    with case(task=materialize_after_dump, value=True):
        # Run DBT to create/update "denuncias" table in "disque_denuncia" dataset
        materialization_flow_id = task_get_flow_group_id(
            flow_name=settings.FLOW_NAME_EXECUTE_DBT_MODEL
        )  # verificar .FLOW_NAME
        materialization_labels = task_get_current_flow_run_labels()

        dump_prod_tables_to_materialize_parameters = [
            {"dataset_id": dataset_id, "table_id": table_id, "dbt_alias": False}
        ]

        dump_prod_materialization_flow_runs = create_flow_run.map(
            flow_id=unmapped(materialization_flow_id),
            parameters=dump_prod_tables_to_materialize_parameters,
            labels=unmapped(materialization_labels),
        )

        dump_prod_materialization_flow_runs.set_upstream(load_to_table_response)

        dump_prod_wait_for_flow_run = wait_for_flow_run.map(
            flow_run_id=dump_prod_materialization_flow_runs,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

template_extracao_fogo_cruzado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
template_extracao_fogo_cruzado.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

with Flow(
    name="CIVITAS: Fogo Cruzado - Atualização",
    state_handlers=[
        handler_inject_bd_credentials,
        # handler_inject_fogocruzado_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as extracao_fogo_cruzado_update:

    materialization_labels = task_get_current_flow_run_labels()

    extracao_fogo_cruzado_update_flow_runs = create_flow_run(
        flow_name=template_extracao_fogo_cruzado.name,
        parameters={"start_date": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")},
        labels=materialization_labels,
    )

    dump_prod_wait_for_flow_run = wait_for_flow_run(
        flow_run_id=extracao_fogo_cruzado_update_flow_runs,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

extracao_fogo_cruzado_update.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extracao_fogo_cruzado_update.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)
extracao_fogo_cruzado_update.schedule = fogo_cruzado_etl_minutely_update_schedule


with Flow(
    name="CIVITAS: Fogo Cruzado - FULL REFRESH",
    state_handlers=[
        handler_inject_bd_credentials,
        # handler_inject_fogocruzado_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as extracao_fogo_cruzado_full_refresh:

    materialization_labels = task_get_current_flow_run_labels()

    extracao_fogo_cruzado_full_refresh_flow_runs = create_flow_run(
        flow_name=template_extracao_fogo_cruzado.name,
        parameters={},
        labels=materialization_labels,
    )

    dump_prod_wait_for_flow_run = wait_for_flow_run(
        flow_run_id=extracao_fogo_cruzado_full_refresh_flow_runs,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

extracao_fogo_cruzado_full_refresh.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extracao_fogo_cruzado_full_refresh.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)
extracao_fogo_cruzado_full_refresh = fogo_cruzado_etl_daily_update_schedule
