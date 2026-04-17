# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT.....
"""

from prefect import Parameter
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_get_current_flow_run_labels,
    task_rename_current_flow_run_dataset_table,
)
from prefeitura_rio.pipelines_utils.state_handlers import handler_skip_if_running
from prefeitura_rio.pipelines_utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_project_name,
)

from pipelines.cerco_digital_staging.radar.schedules import radar_schedule
from pipelines.cerco_digital_staging.radar.tasks import extract_radar_data
from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE, constants
from pipelines.utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_notify_on_failure,
)
from pipelines.utils.tasks import task_get_secret_folder

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: cerco digital - Materialização da tabela de radares CETRIO",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_skip_if_running,
        handler_notify_on_failure,
    ],
) as materialize_radar:
    PIPELINE_SECRETS = task_get_secret_folder(secret_path="/radar")

    #####################################
    # Parameters
    #####################################

    PROJECT_ID = Parameter("project_id", default="rj-civitas")
    DATASET_ID = Parameter("dataset_id", default="cerco_digital")
    TABLE_ID = Parameter("table_id", default="radar")
    DUMP_MODE = Parameter("dump_mode", default="overwrite")

    # DBT
    SELECT = Parameter("select", default="cerco_digital.radar")
    VARS = Parameter("vars", default=None)

    rename_flow_run = task_rename_current_flow_run_dataset_table(
        prefix="ELT_",
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
    )

    #####################################
    # Extract data from CETRIO API and load to BigQuery
    #####################################

    filepath = extract_radar_data(
        secrets=PIPELINE_SECRETS,
        filename="radar_data.csv",
    )

    create_staging_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_mode=DUMP_MODE,
        biglake_table=True,
    )

    #####################################
    # Create Flow Runs
    #####################################

    materialization_labels = task_get_current_flow_run_labels()
    materialization_flow_name = constants.FLOW_NAME_DBT_TRANSFORM.value

    materialization_parameters = [
        {
            "select": SELECT,
            "vars": VARS,
        },
    ]
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

materialize_radar.storage = FLOW_STORAGE
materialize_radar.run_config = FLOW_RUN_CONFIG

materialize_radar.schedule = radar_schedule
