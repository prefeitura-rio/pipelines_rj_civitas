# -*- coding: utf-8 -*-
import io
from datetime import datetime, timedelta

import aiohttp
import discord
import pandas as pd
from discord.utils import MISSING
from pytz import timezone


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
        # If the line contains three crases, it means that a code block is being opened or closed
        if line.strip().startswith("```"):
            is_inside_code_block = not is_inside_code_block  # Alternates between open/closed
            current_code_block_type = line.strip().strip("`")

        # If adding this line to the current chunk does not exceed the limit, we add it
        if len(temp_chunk) + len(line) + 1 <= limit - 4:
            temp_chunk += line + "\n\u200B"
        else:
            # If the chunk ends inside a code block, we close it temporarily
            if is_inside_code_block:
                temp_chunk += "```\u200B"

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
    embeds: list[discord.Embed] = MISSING,
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
            await webhook.send(content=chunks[-1], file=file, username=username, embeds=embeds)

        else:
            await webhook.send(
                content=message, file=file, username=username, avatar_url=avatar_url, embeds=embeds
            )


def get_delay_time_string(df: pd.DataFrame, column_name: str, column_tz: str = "America/Sao_Paulo"):
    """
    Returns a string with the time difference between the current datetime and the datetime
    in the 'column_name' column of the given dataframe.

    Args:
        df (pd.DataFrame): The dataframe with the 'column_name' column.
        column_name (str): The name of the column to get the time difference from.
        column_tz (str, optional): The timezone of the column. Defaults to "America/Sao_Paulo".

    Returns:
        str: A string with the time difference (e.g. "3 dias, 2 horas, 1 minuto e 2 segundos").
    """
    tz = timezone(column_tz)
    datetime_column = pd.to_datetime(df[column_name])
    if datetime_column.tz is None:
        # if the column is not timezone aware, convert it to the specified timezone
        delta: timedelta = datetime.now(tz=tz) - datetime_column.tz_localize(tz)
    else:
        # if the column is timezone aware, convert it to the specified timezone
        delta: timedelta = datetime.now(tz=tz) - datetime_column.dt.tz_convert(tz)

    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Creating a list to store parts of the time string
    time_parts = []

    # Function to add time parts to the list
    def add_time_part(value, singular, plural):
        if value == 1:
            time_parts.append(f"{value} {singular}")
        elif value > 1:
            time_parts.append(f"{value} {plural}")

    # Adding parts for days, hours, minutes, and seconds
    add_time_part(days, "dia", "dias")
    add_time_part(hours, "hora", "horas")
    add_time_part(minutes, "minuto", "minutos")
    add_time_part(seconds, "segundo", "segundos")

    # Joining parts with commas and "and" for the last element
    if time_parts:
        if len(time_parts) == 1:
            time_string = time_parts[0]  # Only one element
        else:
            time_string = ", ".join(time_parts[:-1]) + " e " + time_parts[-1]
    else:
        time_string = "0 segundos"

    return time_string
