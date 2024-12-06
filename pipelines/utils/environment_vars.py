# -*- coding: utf-8 -*-
import os


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
