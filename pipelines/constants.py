# -*- coding: utf-8 -*-
import os
from enum import Enum

from prefect.run_configs import KubernetesRun, LocalRun
from prefect.storage import GCS, Local


class constants(Enum):
    ######################################
    # Automatically managed,
    # please do not change these values
    ######################################
    # Docker image
    DOCKER_TAG = "AUTO_REPLACE_DOCKER_TAG"
    DOCKER_IMAGE_NAME = "AUTO_REPLACE_DOCKER_IMAGE"
    DOCKER_IMAGE = f"{DOCKER_IMAGE_NAME}:{DOCKER_TAG}"
    GCS_FLOWS_BUCKET = "datario-public"

    ######################################
    # Agent labels
    ######################################
    RJ_CIVITAS_AGENT_LABEL = "civitas"

    ######################################
    # Other constants
    ######################################
    # EXAMPLE_CONSTANT = "example_constant"

    FLOW_NAME_DBT_TRANSFORM = "[TEMPLATE] Transformar tabelas com DBT"

    ######################################


if os.getenv("ENVIRONMENT") == "dev":
    FLOW_STORAGE = Local()
    FLOW_RUN_CONFIG = LocalRun(
        labels=[constants.RJ_CIVITAS_AGENT_LABEL.value],
    )
else:
    FLOW_STORAGE = GCS(constants.GCS_FLOWS_BUCKET.value)
    FLOW_RUN_CONFIG = KubernetesRun(
        image=constants.DOCKER_IMAGE.value,
        labels=[constants.RJ_CIVITAS_AGENT_LABEL.value],
    )
