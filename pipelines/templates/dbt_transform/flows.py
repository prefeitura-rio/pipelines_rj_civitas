# -*- coding: utf-8 -*-
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.templates.dbt_transform.tasks import (
    add_dbt_secrets_to_env,
    create_dbt_report,
    execute_dbt,
    get_target_from_environment,
    rename_current_flow_run_dbt,
)
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.tasks import get_current_flow_project_name

with Flow(
    name=constants.FLOW_NAME_DBT_TRANSFORM.value,
) as templates__dbt_transform__flow:

    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)
    SEND_DISCORD_REPORT = Parameter("send_discord_report", default=True)

    # DBT
    COMMAND = Parameter("command", default="test", required=False)
    SELECT = Parameter("select", default=None, required=False)
    EXCLUDE = Parameter("exclude", default=None, required=False)
    FLAG = Parameter("flag", default=None, required=False)
    GITHUB_REPO = Parameter("github_repo", default=None, required=True)
    BIGQUERY_PROJECT = Parameter("bigquery_project", default=None, required=True)
    DBT_SECRETS = Parameter("dbt_secrets", default=[], required=True)
    SECRETS_PATH = Parameter("secrets_path", default=None, required=True)
    ENVIRONMENT = Parameter("environment", default=None)

    #####################################
    # Set environment
    ####################################

    target = get_target_from_environment(environment=ENVIRONMENT)

    current_flow_project_name = get_current_flow_project_name()
    current_flow_project_name.set_upstream(target)

    secrets = add_dbt_secrets_to_env(dbt_secrets=DBT_SECRETS, path=SECRETS_PATH)
    secrets.set_upstream(current_flow_project_name)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run_dbt(
            command=COMMAND, select=SELECT, exclude=EXCLUDE, target=target
        )

    install_dbt_packages = execute_dbt(
        target=target,
        command="deps",
    )
    install_dbt_packages.set_upstream(secrets)

    ####################################
    # Tasks section #1 - Execute commands in DBT
    #####################################

    running_results = execute_dbt(
        target=target,
        command=COMMAND,
        select=SELECT,
        exclude=EXCLUDE,
        flag=FLAG,
        prefect_environment=current_flow_project_name,
    )
    running_results.set_upstream(install_dbt_packages)

    with case(SEND_DISCORD_REPORT, True):
        create_dbt_report_task = create_dbt_report(
            running_results=running_results,
            bigquery_project=BIGQUERY_PROJECT,
        )
        create_dbt_report_task.set_upstream([secrets, running_results])


# # Storage and run configs
templates__dbt_transform__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
templates__dbt_transform__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

# templates__dbt_transform__flow.schedule = dbt_transform_update_schedule

# from prefect.run_configs import LocalRun
# from prefect.storage import Local
# from prefect.executors import LocalDaskExecutor

# templates__dbt_transform__flow.storage = Local()
# templates__dbt_transform__flow.run_config = LocalRun(labels=["dev"])
# templates__dbt_transform__flow.executor = LocalDaskExecutor()