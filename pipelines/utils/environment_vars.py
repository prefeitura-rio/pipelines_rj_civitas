# -*- coding: utf-8 -*-
import os

from prefeitura_rio.pipelines_utils.logging import log


def inject_env_vars(secrets: dict):
    """
    Injects environment variables from a secrets dictionary.

    Args:
        secrets (dict): Dictionary containing secrets to be injected as environment variables.
                       The dictionary keys will be used as environment variable names.

    Returns:
        None
    """
    for key, value in secrets.items():
        os.environ[key] = value


def getenv_or_action(env_name: str, *, action: str = "raise", default: str = None) -> str:
    """Get an environment variable or raise an exception.

    Args:
        env_name (str): The name of the environment variable.
        action (str, optional): The action to take if the environment variable is not set.
            Defaults to "raise".
        default (str, optional): The default value to return if the environment variable is not set.
            Defaults to None.

    Raises:
        ValueError: If the action is not one of "raise", "warn", or "ignore".

    Returns:
        str: The value of the environment variable, or the default value if the environment variable
            is not set.
    """
    if action not in ["raise", "warn", "ignore"]:
        raise ValueError("action must be one of 'raise', 'warn', or 'ignore'")

    value = os.getenv(env_name, default)
    if value is None:
        if action == "raise":
            raise EnvironmentError(f"Environment variable {env_name} is not set.")
        elif action == "warn":
            log(f"Warning: Environment variable {env_name} is not set.", level="warning")
    return value
