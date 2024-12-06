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
    task_check_occurrences_qty,
    task_generate_message,
    task_generate_png_maps,
    task_get_newest_occurrences,
    task_send_discord_messages,
    task_set_config,
)
from pipelines.constants import constants
from pipelines.utils.state_handlers import handler_notify_on_failure
from pipelines.utils.tasks import task_get_secret_folder

with Flow(
    name="CIVITAS: ALERTA DISCORD - Fogo Cruzado",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_notify_on_failure,
    ],
) as alerta_fogo_cruzado:
    start_datetime = Parameter("start_datetime", default="")
    reasons = Parameter("reasons", default=[])

    webhook_url = task_get_secret_folder(secret_path="/discord", inject_env=True)

    config = task_set_config(
        start_datetime=start_datetime, webhook_url=webhook_url, reasons=reasons
    )
    newest_occurrences = task_get_newest_occurrences(config)

    check_response = task_check_occurrences_qty(config)
    check_response.set_upstream(newest_occurrences)

    messages = task_generate_message(config)
    messages.set_upstream(check_response)

    maps = task_generate_png_maps(config, zoom_start=10)
    maps.set_upstream(check_response)

    send_to_discord = task_send_discord_messages(config, upstream_tasks=[messages, maps])

alerta_fogo_cruzado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
alerta_fogo_cruzado.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)
