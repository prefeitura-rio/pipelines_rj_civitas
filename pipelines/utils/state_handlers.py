# -*- coding: utf-8 -*-
import asyncio
import os

from prefect import Flow, Task, context
from prefect.engine import state

from pipelines.utils.notifications import send_discord_message


def handler_notify_on_failure(obj: Flow | Task, old_state: state.State, new_state: state.State):
    """
    State handler that sends a notification to Discord when a Flow or Task fails.

    Args:
        obj (Flow | Task): The Prefect object (Flow or Task) that failed
        old_state (state.State): Previous state of the object
        new_state (state.State): New state of the object that triggered the handler

    Returns:
        state.State: The new state unchanged
    """
    if isinstance(new_state, state.Failed):
        flow_name = context.get("flow_name", "Unknown flow name")
        flow_run_id = context.get("flow_run_id", "Unknown ID")
        project_name = context.get("project_name", "default")
        server_url = os.getenv("PREFECT_UI_URL")

        flow_run_url = f"{server_url}/{project_name}/flow-run/{flow_run_id}?logs"

        new_state_message = f"{new_state.message}\n"
        new_state_message += f"**Flow:** {flow_name}\n"
        new_state_message += f"**URL:** {flow_run_url}\n"

        asyncio.run(
            send_discord_message(
                webhook_url=os.getenv("PIPELINES_RESULTS"),
                message=new_state_message,
                # file=file_bytes,
                # file_format="txt"
            )
        )
