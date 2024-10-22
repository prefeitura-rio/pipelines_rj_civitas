# -*- coding: utf-8 -*-
from enum import Enum


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
    # Redis
    ######################################
    RJ_CIVITAS_REDIS_HOST: str = ("redis-master",)
    RJ_CIVITAS_REDIS_PORT: int = (6379,)
    RJ_CIVITAS_REDIS_DB: int = (0,)  # pylint: disable=C0103

    ######################################
    # Scraping Redes
    ######################################
    SESSION_NAME: str = "telegram_session"
