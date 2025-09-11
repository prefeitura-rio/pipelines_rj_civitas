# -*- coding: utf-8 -*-
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


