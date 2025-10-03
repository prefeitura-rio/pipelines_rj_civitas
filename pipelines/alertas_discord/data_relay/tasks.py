# -*- coding: utf-8 -*-
"""
This module contains tasks for sending Fogo Cruzado ocurrences alerts.
"""
import os
from datetime import timedelta

import requests
from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_flow_run_mode
from prefeitura_rio.pipelines_utils.logging import log


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=10),
)
def check_healthcheck() -> list[dict]:
    """Check API healthcheck endpoint for DLQ status."""
    headers = {"accept": "application/json"}
    log("Checking healthcheck endpoint")
    try:
        response = requests.get(f"{os.getenv('API_URL_BASE')}/health", timeout=10, headers=headers)
        response.raise_for_status()
    except Exception as e:
        log(f"Error checking healthcheck: {e}")
        return []

    log(f"Healthcheck endpoint response: {response.json()}")
    queues = response.json().get("queues", [])
    return queues


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=10),
)
def send_discord_alert(dlq_data: list[dict], dlq_names: list[str]):
    """Send Discord alert if DLQs have messages."""
    environment = get_flow_run_mode()
    dlq_names = [dlq_name.lower() + f"-{environment}_dead" for dlq_name in dlq_names]
    log(f"DLQ names: {dlq_names}")

    dlqs_with_messages = [
        dlq for dlq in dlq_data if dlq.get("messages_ready") > 0 and dlq.get("name") in dlq_names
    ]

    if not dlqs_with_messages:
        log("All DLQs are empty - no alert needed")
        return

    total = sum(dlq.get("messages_ready") for dlq in dlqs_with_messages)
    message = "🚨 **Dead Letter Queue Alert** 🚨\n\n"
    message += f"Found **{len(dlqs_with_messages)} DLQ(s)** with messages:\n\n"

    for dlq in dlqs_with_messages:
        message += f"• `{dlq.get('name')}`: **{dlq.get('messages_ready')} message(s)**\n"

    message += f"\n**Total:** {total} message(s) waiting\n"

    log(f"Sending alert to Discord: {message}")
    response = requests.post(
        os.getenv("DISCORD_WEBHOOK_URL_DATA_RELAY_DLQ_MONITOR"), json={"content": message}
    )
    response.raise_for_status()
    log(f"Alert sent successfully for {len(dlqs_with_messages)} DLQ(s)")
