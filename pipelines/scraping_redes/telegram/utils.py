# -*- coding: utf-8 -*-
# import json
# import asyncio
from datetime import datetime
from typing import Dict, Optional

import basedosdados as bd
import pytz

# from google.cloud import bigquery
# from redis_pal import RedisPal
from telethon import TelegramClient

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True

tz = pytz.timezone("America/Sao_Paulo")


async def get_channel_info(
    api_id: str, api_hash: str, phone_number: str, channel_username: str
) -> Optional[Dict]:
    """
    Retorna informações sobre um canal do Telegram dado seu username.

    Args:
        api_id: ID da API do Telegram
        api_hash: Hash da API do Telegram
        phone_number: Número de telefone associado à conta
        channel_username: Username do canal (sem @)

    Returns:
        Dict com informações do canal ou None se não encontrado
    """

    client = TelegramClient("telegram_scraping", api_id, api_hash)

    try:
        await client.start(phone=phone_number)

        # Obtém a entidade do canal
        channel = await client.get_entity(channel_username)

        # Formata o timestamp da criação
        tz = pytz.timezone("America/Sao_Paulo")
        created_at = datetime.fromtimestamp(channel.date.timestamp(), tz=tz)
        created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")

        channel_info = {
            "id": channel.id,
            "username": channel.username,
            "title": channel.title,
            "participants_count": (await client.get_participants(channel, limit=0)).total,
            "created_at": created_at_str,
            "verified": channel.verified if hasattr(channel, "verified") else False,
            "description": channel.about if hasattr(channel, "about") else None,
            "linked_chat_id": (
                channel.linked_chat_id if hasattr(channel, "linked_chat_id") else None
            ),
            "is_private": channel.broadcast if hasattr(channel, "broadcast") else None,
        }

        return channel_info

    except Exception as e:
        print(f"Erro ao obter informações do canal {channel_username}: {str(e)}")
        return None

    finally:
        await client.disconnect()


async def get_multiple_channels_info(
    api_id: str, api_hash: str, phone_number: str, channel_usernames: list[str]
) -> Dict[str, Dict]:
    """
    Retorna informações sobre múltiplos canais do Telegram.

    Args:
        api_id: ID da API do Telegram
        api_hash: Hash da API do Telegram
        phone_number: Número de telefone associado à conta
        channel_usernames: Lista de usernames dos canais (sem @)

    Returns:
        Dict com informações de todos os canais
    """

    # channels_info = {}
    channels_info = []

    for username in channel_usernames:
        info = await get_channel_info(
            api_id=api_id, api_hash=api_hash, phone_number=phone_number, channel_username=username
        )
        if info:
            # channels_info[username] = info
            channels_info.append(info)

    return channels_info
