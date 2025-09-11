# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data.....
"""

from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_get_current_flow_run_labels,
    task_rename_current_flow_run_dataset_table,
)
from prefeitura_rio.pipelines_utils.state_handlers import handler_skip_if_running
from prefeitura_rio.pipelines_utils.tasks import get_current_flow_project_name

from pipelines.cerco_digital.cameras.schedules import cerco_digital_cameras_schedule
from pipelines.cerco_digital.cameras.tasks import (
    create_table_and_upload_to_gcs,
    get_cameras,
)
from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE, constants
from pipelines.utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_notify_on_failure,
)
from pipelines.utils.tasks import task_get_secret_folder

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: Cerco Digital - Cameras",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_skip_if_running,
        handler_notify_on_failure,
    ],
) as elt_cerco_digital_cameras:

    #####################################
    # Parameters
    #####################################

    # Extraction
    DATASET_ID = Parameter("dataset_id", default="cerco_digital")
    TABLE_ID = Parameter("table_id", default="cameras")
    DUMP_MODE = Parameter("dump_mode", default="overwrite")
    BIGLAKE_TABLE = Parameter("biglake_table", default=False)

    SECRETS = task_get_secret_folder(secret_path="/api-tixxi", inject_env=True)

    rename_flow_run = task_rename_current_flow_run_dataset_table(
        prefix="ELT_", dataset_id=DATASET_ID, table_id=TABLE_ID
    )

    #####################################
    # EXTRACT AND LOAD
    #####################################
    cameras = get_cameras()
    cameras.set_upstream([SECRETS, rename_flow_run])

    upload_raw_data = create_table_and_upload_to_gcs(
        cameras=cameras,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_mode=DUMP_MODE,
        biglake_table=BIGLAKE_TABLE,
    )
    upload_raw_data.set_upstream(cameras)

    #####################################
    # TRANSFORM
    #####################################
    materialization_labels = task_get_current_flow_run_labels()
    materialization_flow_name = constants.FLOW_NAME_DBT_TRANSFORM.value

    materialization_parameters = [
        {
            "select": TABLE_ID,
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
    materialization_flow_runs.set_upstream(upload_raw_data)

    materialization_wait_for_flow_run = wait_for_flow_run.map(
        flow_run_id=materialization_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )


elt_cerco_digital_cameras.storage = FLOW_STORAGE
elt_cerco_digital_cameras.run_config = FLOW_RUN_CONFIG

elt_cerco_digital_cameras.schedule = cerco_digital_cameras_schedule
