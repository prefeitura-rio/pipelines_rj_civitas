# -*- coding: utf-8 -*-
"""
Send a discord alert whenever a new message is detected in the Data Relay DLQ..
"""
from prefect import Parameter
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.alertas_discord.data_relay.schedules import (
    alertas_data_relay_dlq_monitor_5min_update_schedule,
)
from pipelines.alertas_discord.data_relay.tasks import (
    check_healthcheck,
    send_discord_alert,
)
from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE
from pipelines.utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_notify_on_failure,
)
from pipelines.utils.tasks import task_get_secret_folder

with Flow(
    name="CIVITAS: ALERTA DISCORD - Data Relay DLQ Monitor",
    state_handlers=[
        handler_inject_bd_credentials,
        # handler_initialize_sentry,
        handler_notify_on_failure,
    ],
) as alerta_data_relay_dlq_monitor:

    # Parameters
    DLQ_NAMES = Parameter("dlq_names", default=[])

    # Secrets
    DATA_RELAY_SECRETS = task_get_secret_folder(secret_path="/api-data-relay", inject_env=True)
    DISCORD_SECRETS = task_get_secret_folder(secret_path="/discord", inject_env=True)

    # Check healthcheck
    health_data = check_healthcheck()
    health_data.set_upstream([DATA_RELAY_SECRETS, DISCORD_SECRETS])

    # Send alert if needed
    alert = send_discord_alert(health_data, DLQ_NAMES)
    alert.set_upstream(health_data)

alerta_data_relay_dlq_monitor.storage = FLOW_STORAGE
alerta_data_relay_dlq_monitor.run_config = FLOW_RUN_CONFIG
alerta_data_relay_dlq_monitor.schedule = alertas_data_relay_dlq_monitor_5min_update_schedule
