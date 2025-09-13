# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT.....
"""

from prefect import Parameter
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import task_get_current_flow_run_labels
from prefeitura_rio.pipelines_utils.state_handlers import handler_skip_if_running
from prefeitura_rio.pipelines_utils.tasks import get_current_flow_project_name

from pipelines.cerco_digital.readings.schedules import readings_schedule
from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE, constants
from pipelines.utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_notify_on_failure,
)
from pipelines.utils.tasks import task_get_secret_folder

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: cerco digital - Materialização da view de leituras de placas",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_skip_if_running,
        handler_notify_on_failure,
    ],
) as materialize_readings:

    DISCORD_SECRETS = task_get_secret_folder(secret_path="/discord", inject_env=True)

    #####################################
    # Parameters
    #####################################

    # DBT
    SELECT = Parameter("select", default=None)
    VARS = Parameter("vars", default=None)

    materialization_labels = task_get_current_flow_run_labels()
    materialization_flow_name = constants.FLOW_NAME_DBT_TRANSFORM.value

    materialization_parameters = [
        {
            "select": SELECT,
            "vars": VARS,
        },
    ]

    #####################################
    # Create Flow Runs
    #####################################

    current_flow_project_name = get_current_flow_project_name()

    materialization_flow_runs = create_flow_run.map(
        flow_name=unmapped(materialization_flow_name),
        project_name=unmapped(current_flow_project_name),
        parameters=materialization_parameters,
        labels=unmapped(materialization_labels),
    )

    materialization_wait_for_flow_run = wait_for_flow_run.map(
        flow_run_id=materialization_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

#####################################
# Set Storage and Run Config
#####################################

materialize_readings.storage = FLOW_STORAGE
materialize_readings.run_config = FLOW_RUN_CONFIG

materialize_readings.schedule = readings_schedule
