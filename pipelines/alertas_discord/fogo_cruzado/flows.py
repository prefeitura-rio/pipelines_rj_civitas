# -*- coding: utf-8 -*-
"""
Send a discord alert whenever a new occurrence is detected in the Fogo Cruzado.
"""
from prefect import Parameter
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.alertas_discord.fogo_cruzado.tasks import (
    task_check_occurrences_qty,
    task_generate_message,
    task_generate_png_maps,
    task_get_newest_occurrences,
    task_send_discord_messages,
    task_set_config,
)
from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE
from pipelines.utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_notify_on_failure,
)
from pipelines.utils.tasks import task_get_secret_folder

with Flow(
    name="CIVITAS: ALERTA DISCORD - Fogo Cruzado",
    state_handlers=[
        handler_inject_bd_credentials,
        # handler_initialize_sentry,
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

alerta_fogo_cruzado.storage = FLOW_STORAGE
alerta_fogo_cruzado.run_config = FLOW_RUN_CONFIG
