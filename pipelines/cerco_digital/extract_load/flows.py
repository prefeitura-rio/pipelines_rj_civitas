# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT.
related to 'cerco_digital.licenciamento_veiculos'.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import task_get_current_flow_run_labels
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)
from prefeitura_rio.pipelines_utils.tasks import get_current_flow_project_name

from pipelines.cerco_digital.extract_load.schedules import (
    licenciamento_veiculos_daily_update_schedule,
)
from pipelines.constants import constants
from pipelines.utils.state_handlers import handler_notify_on_failure
from pipelines.utils.tasks import task_get_secret_folder

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: cerco digital - Materialização dos dados de licenciamento de veículos",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
        handler_notify_on_failure,
    ],
) as materialize_licenciamento_veiculos:

    secrets = task_get_secret_folder(secret_path="/discord", inject_env=True)

    dataset_id = Parameter("dataset_id", default="cerco_digital")
    table_id = Parameter("table_id", default="licenciamento_veiculos")
    dbt_alias = Parameter("dbt_alias", default=False)

    materialization_flow_name = settings.FLOW_NAME_EXECUTE_DBT_MODEL
    materialization_labels = task_get_current_flow_run_labels()

    dump_prod_tables_to_materialize_parameters = [
        {"dataset_id": dataset_id, "table_id": table_id, "dbt_alias": dbt_alias}
    ]
    current_flow_project_name = get_current_flow_project_name()

    dump_prod_materialization_flow_runs = create_flow_run.map(
        flow_name=unmapped(materialization_flow_name),
        project_name=unmapped(current_flow_project_name),
        parameters=dump_prod_tables_to_materialize_parameters,
        labels=unmapped(materialization_labels),
    )

    dump_prod_wait_for_flow_run = wait_for_flow_run.map(
        flow_run_id=dump_prod_materialization_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

materialize_licenciamento_veiculos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_licenciamento_veiculos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

materialize_licenciamento_veiculos.schedule = licenciamento_veiculos_daily_update_schedule
