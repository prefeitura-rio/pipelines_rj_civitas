# -*- coding: utf-8 -*-

import uuid
from typing import Literal

from infisical import InfisicalClient
from prefect import task
from prefect.backend import FlowRunView
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.environment_vars import inject_env_vars
from pipelines.utils.state_handlers import handler_save_traceback_on_failure


@task(state_handlers=[handler_save_traceback_on_failure])
def task_inject_env(
    secret_name: str = None,
    environment: str = None,
    secret_path: str = "/",
    inject_env: bool = True,
    type: Literal["shared", "personal"] = "personal",
    client: InfisicalClient = None,
) -> dict:
    """
    Fetches secrets from Infisical. If passing only `secret_path` and
    no `secret_name`, returns all secrets inside a folder.
    Args:
        secret_path (str, optional): Path to the secrets folder. Defaults to '/'.
        secret_name (str, optional): Name of the specific secret to fetch. Defaults to None.
        type (Literal["shared", "personal"], optional): Type of secret. Defaults to "personal".
        environment (str, optional): Environment to fetch secrets from. Defaults to None.
        client (InfisicalClient, optional): Infisical client instance. Defaults to None.
        inject_env_vars (bool, optional): Whether to inject secrets as environment variables.
        Defaults to True.
    Returns:
        dict: Dictionary containing the fetched secrets
    """
    log(
        f"Fetching secrets from Infisical for path: {secret_path}, "
        f"name: {secret_name}, type: {type}, environment: {environment}"
    )
    secret = get_secret(
        secret_name=secret_name, environment=environment, type=type, path=secret_path, client=client
    )

    if inject_env:
        log("Injecting secrets as environment variables")
        inject_env_vars(secret)

    return secret


@task(state_handlers=[handler_save_traceback_on_failure])
def task_get_flow_run_logs(flow_run_id: uuid.UUID):
    """
    Fetches the logs of a specific flow run.
    Args:
        flow_run_id (str): The ID of the flow run to fetch logs for.
    Returns:
        list: A list of log messages.
    """
    flow_run = FlowRunView.from_flow_run_id(flow_run_id)
    logs = flow_run.get_logs()
    log(f"logs: {logs}")

    logs_message = [f"level:{log.level}\nmensagem: {log.message}" for log in logs]
    final_log_message = "\n".join(logs_message)

    return final_log_message
