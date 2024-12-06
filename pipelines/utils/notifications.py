# -*- coding: utf-8 -*-
import io

import aiohttp
import discord


async def send_discord_message(
    webhook_url: str,
    message: str,
    file: bytes = None,
    file_format: str = None,
    username: str = None,
    avatar_url: str = None,
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
    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(webhook_url, session=session)
        if file:
            file = discord.File(io.BytesIO(file), filename="attachment." + file_format)

            await webhook.send(content=message, file=file, username=username, avatar_url=avatar_url)
        else:
            await webhook.send(content=message)
