# -*- coding: utf-8 -*-

import asyncio
import io
import os
import traceback
from pathlib import Path
from textwrap import dedent
from typing import Any

import aiohttp
import discord
from prefect import Flow, Task, context

# from prefect.triggers import all_finished
from prefect.backend import FlowRunView
from prefect.engine import state
from prefect.engine.state import Failed
from prefeitura_rio.pipelines_utils.logging import log

MISSING: Any = discord.utils.MISSING


def handler_save_traceback_on_failure(obj, old_state, new_state):
    if isinstance(new_state, Failed):
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

        file_dir = Path.cwd() / "pipelines_logs"
        file_dir.mkdir(parents=True, exist_ok=True)
        file_name = f'{context.get("flow_run_id")}.txt'

        with open(file_dir / file_name, "a") as f:  # TODO: generate a unique file name
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
    if isinstance(new_state, state.Failed):
        if isinstance(obj, Flow):

            flow_run_id = context.get("flow_run_id")
            flow_name = context.get("flow_name")
            log(f"flow_run_id: {flow_run_id}")
            log(f"flow_name: {flow_name}")

            new_state_message = f"**Flow:** {flow_name}\n"
            new_state_message += f"flow_run_id: {flow_run_id}\n"

            flow_run = FlowRunView.from_flow_run_id(flow_run_id)

            logs = flow_run.get_logs()

            logs_message = [f"level:{log.level}\nmensagem: {log.message}" for log in logs]
            final_log_message = "\n".join(logs_message)

            if logs:
                new_state_message += f"\n```bash\n{final_log_message}\n```"

        asyncio.run(
            send_discord_message(
                webhook_url=os.getenv("PIPELINES_RESULTS"),
                message=new_state_message,
                username="Prefect",
            )
        )


def split_by_newline(text: str, limit: int = 2000) -> list[str]:
    """
    Split a text into smaller chunks by newlines while respecting a character limit,
    ensuring that code blocks (``` ... ```) are not broken incorrectly.
    Args:
        text (str): The text to be split.
        limit (int, optional): Maximum number of characters per chunk. Defaults to 2000.
    Returns:
        list[str]: List containing the split text chunks
    """
    chunks = []
    is_inside_code_block = False
    current_code_block_type = None  # Variable to store the type of code block (e.g., bash)
    temp_chunk = ""

    for line in text.split("\n"):
        # If the line contains three backticks, it means that a code block is being opened or closed
        if line.strip().startswith("```"):
            is_inside_code_block = not is_inside_code_block  # Alternates between open/closed
            current_code_block_type = line.strip().strip("`")

        # If adding this line to the current chunk does not exceed the limit, we add it
        if len(temp_chunk) + len(line) + 1 <= limit - 3:
            temp_chunk += line + "\n"
        else:
            # If the chunk ends inside a code block, we close it temporarily
            if is_inside_code_block:
                temp_chunk += "```"

            chunks.append(temp_chunk.strip())

            # If the new chunk starts inside a code block, we reopen it
            temp_chunk = (
                "```" + current_code_block_type + "\n" + line + "\n"
                if is_inside_code_block
                else line + "\n"
            )

    if temp_chunk:
        chunks.append(temp_chunk.strip())

    return chunks


async def send_discord_message(
    webhook_url: str,
    message: str,
    file: bytes = MISSING,
    file_format: str = None,
    username: str = MISSING,
    avatar_url: str = MISSING,
):
    """Sends a message to a Discord webhook.
    Args:
        webhook_url (str): The Discord webhook URL.
        message (str): The message to be sent.
        file (bytes, optional): Binary data of the file to be attached.
        file_format (str, optional): Format of the file to be attached (e.g. 'png', 'txt').
        username (str, optional): Custom username for the webhook.
        avatar_url (str, optional): Custom avatar URL for the webhook.
    """
    chunks = split_by_newline(message)

    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(webhook_url, session=session)

        if file:
            file = discord.File(io.BytesIO(file), filename="attachment." + file_format)

        if len(chunks) > 1:
            # Send the first chunk with username and avatar
            await webhook.send(content=chunks[0], username=username, avatar_url=avatar_url)

            # Send the middle chunks without avatar
            for chunk in chunks[1:-1]:
                await webhook.send(content=chunk, username=username)

            # Send the last chunk with file
            await webhook.send(content=chunks[-1], file=file, username=username)

        else:
            await webhook.send(content=message, file=file, username=username, avatar_url=avatar_url)
