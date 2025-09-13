# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT.
related to 'cerco_digital.licenciamento_veiculos'.
"""
from prefect import Parameter
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import task_get_current_flow_run_labels
from prefeitura_rio.pipelines_utils.state_handlers import (  # handler_initialize_sentry,
    handler_skip_if_running,
)
from prefeitura_rio.pipelines_utils.tasks import get_current_flow_project_name

from pipelines.cerco_digital.auxiliary_tables.schedules import (
    auxiliary_tables_daily_update_schedule,
)
from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE, constants
from pipelines.utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_notify_on_failure,
)
from pipelines.utils.tasks import task_get_secret_folder

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: cerco digital - Materialização das tabelas auxiliares",
    state_handlers=[
        handler_inject_bd_credentials,
        # handler_initialize_sentry,
        handler_skip_if_running,
        handler_notify_on_failure,
    ],
) as materialize_auxiliary_tables:

    SECRETS = task_get_secret_folder(secret_path="/discord", inject_env=True)

    DATASET_ID = Parameter("dataset_id", default="cerco_digital")
    EXCLUDE = Parameter("exclude", default="vw_readings")

    materialization_flow_name = constants.FLOW_NAME_DBT_TRANSFORM.value
    materialization_labels = task_get_current_flow_run_labels()

    dump_prod_tables_to_materialize_parameters = [
        {
            "select": DATASET_ID,
            "exclude": EXCLUDE,
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

materialize_auxiliary_tables.storage = FLOW_STORAGE
materialize_auxiliary_tables.run_config = FLOW_RUN_CONFIG

materialize_auxiliary_tables.schedule = auxiliary_tables_daily_update_schedule
