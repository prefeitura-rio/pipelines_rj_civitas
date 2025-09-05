# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT.
related to 'disque_denuncia.denuncias'..
"""


from prefect import Parameter
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (  # get_flow_run_mode,
    task_get_current_flow_run_labels,
)
from prefeitura_rio.pipelines_utils.state_handlers import (
    # handler_initialize_sentry,
    handler_skip_if_running,
)
from pipelines.utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import (  # task_run_dbt_model_task,
    get_current_flow_project_name,
)

from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE, constants
from pipelines.utils.state_handlers import handler_notify_on_failure
from pipelines.utils.tasks import task_get_secret_folder

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: integracao_reports_staging - Materialize disque denuncia",
    state_handlers=[
        handler_inject_bd_credentials,
        # handler_initialize_sentry,
        handler_skip_if_running,
        handler_notify_on_failure,
    ],
) as materialize_integracao_reports_disque_denuncia:

    secrets = task_get_secret_folder(secret_path="/discord", inject_env=True)

    TABLE_ID = Parameter("table_id", default="reports_disque_denuncia")

    materialization_flow_name = constants.FLOW_NAME_DBT_TRANSFORM.value
    materialization_labels = task_get_current_flow_run_labels()

    dump_prod_tables_to_materialize_parameters = [
        {
            "select": TABLE_ID,
        }
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

materialize_integracao_reports_disque_denuncia.storage = FLOW_STORAGE
materialize_integracao_reports_disque_denuncia.run_config = FLOW_RUN_CONFIG