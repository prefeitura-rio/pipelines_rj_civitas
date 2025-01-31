# -*- coding: utf-8 -*-

from typing import Literal

from infisical import InfisicalClient
from prefect import task
from prefect.triggers import all_finished
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.environment_vars import inject_env_vars


@task
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


# @task(state_handlers=[handler_save_traceback_on_failure])
@task
def task_that_fails():
    a = 1
    b = 0

    try:
        c = a / b
        log(c)
    except ZeroDivisionError as e:
        raise ZeroDivisionError("Simulated task failure") from e


# @task(state_handlers=[handler_save_traceback_on_failure], trigger=all_finished)
@task(trigger=all_finished)
def task_that_fails_2():
    a: int = "asdad"

    try:
        b = 10 / a
        log(b)
    except TypeError as e:
        raise TypeError("Simulated task failure 2") from e


@task
def task_that_succeeds():
    pass
