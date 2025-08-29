# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT.....
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# from prefect.executors import LocalDaskExecutor
# from prefect.run_configs import LocalRun
# from prefect.storage import Local
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (  # get_flow_run_mode,
    task_get_current_flow_run_labels,
)
from prefeitura_rio.pipelines_utils.state_handlers import (  # handler_inject_bd_credentials,
    handler_initialize_sentry,
    handler_skip_if_running,
)
from prefeitura_rio.pipelines_utils.tasks import get_current_flow_project_name
from tmp.pipelines_playground.cerco_digital.materialize_new.tasks import (  # TODO: remove this
    task_remove_dbt_log_file,
)

from pipelines import constants
from pipelines.cerco_digital.readings.schedules import readings_schedule
from pipelines.constants import constants
from pipelines.utils.state_handlers import handler_inject_bd_credentials

# from pipelines.utils.state_handlers import handler_notify_on_failure
# from pipelines.templates.dbt_transform.tasks import get_target_from_environment
# from pipelines.utils.tasks import task_get_secret_folder

# from tmp.pipelines_playground.cerco_digital.materialize_new.tasks import task_remove_dbt_log_file # TODO: remove this

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: cerco digital - Materialização da view de leituras de placas",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
        # handler_notify_on_failure,
    ],
) as materialize_readings:

    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=True)
    SEND_DISCORD_REPORT = Parameter("send_discord_report", default=True)

    # DBT
    COMMAND = Parameter("command", default=None)
    SELECT = Parameter("select", default=None)
    GITHUB_REPO = Parameter("github_repo", default=None)
    BIGQUERY_PROJECT = Parameter("bigquery_project", default=None)
    DBT_SECRETS = Parameter("dbt_secrets", default=[])

    # Environment
    ENVIRONMENT = Parameter("environment", default=None)
    SECRETS_PATH = Parameter("secrets_path", default=None)

    materialization_labels = task_get_current_flow_run_labels()
    materialization_flow_name = constants.FLOW_NAME_DBT_TRANSFORM.value
        
    # remove_dbt_log_file = task_remove_dbt_log_file() # TODO: remove this
    
    materialization_parameters = [
        {
            "rename_flow": RENAME_FLOW,
            "send_discord_report": SEND_DISCORD_REPORT,
            "command": COMMAND,
            "select": SELECT,
            "github_repo": GITHUB_REPO,
            "bigquery_project": BIGQUERY_PROJECT,
            "dbt_secrets": DBT_SECRETS,
            "secrets_path": SECRETS_PATH,
            "environment": ENVIRONMENT,
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

materialize_readings.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_readings.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

materialize_readings.schedule = readings_schedule

# materialize_readings.storage = Local()
# materialize_readings.run_config = LocalRun(labels=["dev"])
# materialize_readings.executor = LocalDaskExecutor()
