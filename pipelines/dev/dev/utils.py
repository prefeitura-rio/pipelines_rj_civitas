# -*- coding: utf-8 -*-
import asyncio
import io
import os
import re
from os import environ

import aiohttp
import discord
from prefect import Task, context
from prefect.engine import state
from prefeitura_rio.pipelines_utils.logging import log


def inject_env_vars(secrets: dict):
    for key, value in secrets.items():
        environ[key] = value


async def send_discord_message(
    webhook_url: str,
    message: str,
    # image_data: bytes = None,
    file: bytes = None,
    file_format: str = None,
    username: str = None,
    avatar_url: str = None,
):
    """Sends a message to a Discord webhook.

    Args:
        webhook_url (str): The URL of the webhook.
        message (str): The message to send.
        image_data (bytes, optional): The PNG image data to embed.
    """
    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(webhook_url, session=session)
        if file:
            file = discord.File(io.BytesIO(file), filename="attachment." + file_format)

            await webhook.send(content=message, file=file, username=username, avatar_url=avatar_url)
        else:
            await webhook.send(content=message)


def notify_on_failure(task: Task, old_state: state.State, new_state: state.State):
    if isinstance(new_state, state.Failed):
        # Capture all error messages and logs
        error_dict = new_state.result
        log(f"error_dict: {dir(error_dict)}")
        log(f"new state results: {new_state.result}")
        log(f"new state: {type(new_state)}")

        error_state = list(error_dict.values())[0]  # get first value from dictionary
        error_logs = str(error_state.result)  # get exception text

        # Get current flow name
        flow_name = context.get("flow_name", "Unknown flow name")
        flow_run_id = context.get("flow_run_id", "Unknown ID")
        project_name = context.get("project_name", "default")  # Project/team name
        server_url = context.get("server_url", "not found")

        pattern = r"<(.*?)>"

        new_state_results = re.findall(pattern, str(new_state.result))
        log(f"new_state_results: {new_state_results}")
        # Get Prefect server base URL

        # Build flow run URL using context information
        flow_run_url = f"{server_url}/{project_name}/flow-run/{flow_run_id}?logs"

        new_state_message = f"**Fluxo:** {flow_name}\n"
        new_state_message += f"**ID:** {flow_run_id}\n"
        new_state_message += f"**URL:** {flow_run_url}\n"

        new_state_message += new_state_results[0]
        new_state_message += """```bash\n""" + error_logs + """\n```"""

        log(new_state_message)

        asyncio.run(
            send_discord_message(
                webhook_url=os.getenv("PIPELINES_RESULTS"),
                message=new_state_message,
                # file=file_bytes,
                # file_format="txt",
                username="Prefect - TESTE",
                avatar_url="https://img.icons8.com/?size=100&id=63688&format=png&color=000000",
            )
        )
