# -*- coding: utf-8 -*-
"""
Send a discord alert whenever a new occurrence is detected in the Fogo Cruzado.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.alertas_discord.fogo_cruzado.tasks import (
    generate_message,
    get_newest_occurrences,
    task_generate_png_maps,
    task_send_discord_messages,
)
from pipelines.constants import constants

with Flow(
    name="CIVITAS: ALERTA DISCORD - Fogo Cruzado",
    state_handlers=[handler_inject_bd_credentials, handler_initialize_sentry],
) as alerta_fogo_cruzado:
    start_datetime = Parameter("start_datetime", default="")
    webhook_url = Parameter("webhook_url", default="")

    newest_occurrences = get_newest_occurrences(start_datetime=start_datetime)

    messages = generate_message(newest_occurrences=newest_occurrences)
    maps = task_generate_png_maps(occurrences=newest_occurrences, zoom_start=20)

    send_to_discord = task_send_discord_messages(
        webhook_url=webhook_url, messages=messages, images=maps
    )

alerta_fogo_cruzado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
alerta_fogo_cruzado.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)
