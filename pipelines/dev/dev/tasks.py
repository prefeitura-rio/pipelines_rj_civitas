# -*- coding: utf-8 -*-
# import logging
import traceback
from typing import Literal

from infisical import InfisicalClient
from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_secret_folder
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.dev.dev.utils import inject_env_vars


@task
def task_get_secret_folder(
    secret_path: str = "/",
    secret_name: str = None,
    type: Literal["shared", "personal"] = "personal",
    environment: str = None,
    client: InfisicalClient = None,
) -> dict:
    """
    Fetches secrets from Infisical. If passing only `secret_path` and
    no `secret_name`, returns all secrets inside a folder.

    Args:
        secret_name (str, optional): _description_. Defaults to None.
        secret_path (str, optional): _description_. Defaults to '/'.
        environment (str, optional): _description_. Defaults to 'dev'.

    Returns:
        _type_: _description_
    """
    secrets = get_secret_folder(
        secret_path=secret_path,
        secret_name=secret_name,
        type=type,
        environment=environment,
        client=client,
    )

    inject_env_vars(secrets)
    # return secrets


@task()
def my_task():
    log("Starting task")
    try:
        result = 1 / 0
        return result

    except ZeroDivisionError as e:
        error_traceback = traceback.format_exc(chain=True)
        log(f">>>>>>>>>>>>>>>>>>>>>>> {error_traceback}")
        # creating a dictionary with the error information
        error_info = {"error": str(e), "traceback": error_traceback, "type": type(e).__name__}
        raise Exception(str(error_info))  # Casting to string to ensure serialization
    # pass
