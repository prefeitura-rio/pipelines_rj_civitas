# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT.....
"""

from prefect import Parameter
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_rename_current_flow_run_dataset_table,
)
from prefeitura_rio.pipelines_utils.state_handlers import handler_skip_if_running
from prefeitura_rio.pipelines_utils.tasks import create_table_and_upload_to_gcs

from pipelines.cerco_digital_staging.radar.tasks import extract_radar_data
from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE
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

    DATASET_ID = Parameter("dataset_id", default="cerco_digital")
    TABLE_ID = Parameter("table_id", default="radar")
    DUMP_MODE = Parameter("dump_mode", default="overwrite")

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
# Set Storage and Run Config
#####################################

materialize_radar.storage = FLOW_STORAGE
materialize_radar.run_config = FLOW_RUN_CONFIG
