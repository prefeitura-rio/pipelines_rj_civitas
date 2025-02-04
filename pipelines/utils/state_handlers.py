# -*- coding: utf-8 -*-
import asyncio
import logging
import os
import time
import traceback
from pathlib import Path
from textwrap import dedent

import pendulum
from prefect import Flow, Task, context
from prefect.backend import FlowRunView
from prefect.engine import state
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.notifications import send_discord_message


def handler_save_traceback_on_failure(obj, old_state, new_state):
    if isinstance(new_state, state.Failed):
        exc_info = (type(new_state.result), new_state.result, new_state.result.__traceback__)
        full_traceback = "".join(traceback.format_exception(*exc_info))

        if "raise signals.TRIGGERFAIL" in full_traceback:
            return None

        task_name = obj.name if isinstance(obj, Task) else "Flow-level failure"
        task_message = str(new_state.message)
        task_cached_inputs = str(new_state.cached_inputs)

        trigger = obj.trigger
        error_description = dedent(
            f"""\

            :x: **Task name:** {task_name}
            **Trigger:** {trigger.__name__}

            **Task message:**
            {task_message}

            **Task cached inputs:**
            {task_cached_inputs}
            """
        )
        error_description += f"\n```bash\n{full_traceback}\n```"

        file_dir = Path("/tmp/pipelines/error_logs")
        file_dir.mkdir(parents=True, exist_ok=True)
        file_name = f'{context.get("flow_run_id")}.txt'

        with open(file_dir / file_name, "a") as f:
            f.write(str(error_description))


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
    if isinstance(new_state, state.Failed) and isinstance(obj, Flow):

        server_url = os.getenv("PREFECT_UI_URL")
        project_name = context.get("project_name", "default")
        flow_run_id = context.get("flow_run_id", "Unknown flow run ID")
        flow_name = context.get("flow_name", "Unknown flow name")
        flow_run_url = f"{server_url}/{project_name}/flow-run/{flow_run_id}?logs"

        new_state_message = f"**Flow:** {flow_name}\n"
        new_state_message += f"flow_run_id: {flow_run_id}\n"
        new_state_message += f"**URL:** {flow_run_url}\n"

        time.sleep(10)  # Await for all logs to be available
        log(f"CURRENT_TIME: {pendulum.now(tz=pendulum.UTC)}", level="debug")
        flow_run = FlowRunView.from_flow_run_id(flow_run_id)

        logs = flow_run.get_logs(end_time=pendulum.now(tz=pendulum.UTC))

        if logs:
            log(f"logs: {logs}", level="debug")

            logs_message = [
                f"level: {logging.getLevelName(i.level)}\n" f"mensagem: {i.message}"
                for i in logs
                if i.level in [30, 40, 50]
            ]
            log(f"LOGS_MESSAGE: {logs_message}", level="debug")

            if logs_message:
                final_log_message = "\n".join(logs_message)
                new_state_message += f"\n```bash\n{final_log_message}\n```"

        asyncio.run(
            send_discord_message(
                webhook_url=os.getenv("PIPELINES_RESULTS"),
                message=new_state_message,
                username="Prefect",
            )
        )
