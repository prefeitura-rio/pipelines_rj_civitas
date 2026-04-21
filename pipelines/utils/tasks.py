# -*- coding: utf-8 -*-
from datetime import datetime, timezone
from typing import Literal

from infisical import InfisicalClient
from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_secret_folder
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.environment_vars import inject_env_vars


@task
def task_get_secret_folder(
    secret_path: str = "/",
    secret_name: str = None,
    type: Literal["shared", "personal"] = "personal",
    environment: str = None,
    client: InfisicalClient = None,
    inject_env: bool = True,
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
    secrets = get_secret_folder(
        secret_path=secret_path,
        secret_name=secret_name,
        type=type,
        environment=environment,
        client=client,
    )

    if inject_env:
        log("Injecting secrets as environment variables")
        inject_env_vars(secrets)

    return secrets


@task
def add_default_start_date_to_dbt_vars(vars: list[dict]) -> list[dict]:
    """
    Adds a default start_date to the vars list.

    The default start_date is the current hour with minute, second and microsecond set to 0.

    Args:
        vars (list[dict]): The list of variables to add the default start_date to.

    Returns:
        list[dict]: The list of variables with the default start_date added.
    """
    now = datetime.now(tz=timezone.utc)
    start_date = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:00:00")
    result = list(vars) if vars else []
    result.append({"start_date": start_date})
    return result
