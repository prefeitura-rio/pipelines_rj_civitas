# -*- coding: utf-8 -*-
# import json
import base64
from datetime import datetime
from typing import Any, Dict, List, Literal

import basedosdados as bd
import pytz
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log
from telethon import TelegramClient

from pipelines.scraping_redes.telegram.models.telegram import Channel, Message
from pipelines.utils import get_redis_client

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True

tz = pytz.timezone("America/Sao_Paulo")


def save_data_in_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    schema: List[bigquery.SchemaField],
    json_data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
) -> None:
    """
    Saves a list of dictionaries to a BigQuery table.

    Args:
        project_id: The ID of the GCP project.
        dataset_id: The ID of the dataset.
        table_id: The ID of the table.
        schema: List of BigQuery table schema.
        json_data: The list of dictionaries to be saved to BigQuery.

    Raises:
        Exception: If there is an error while inserting the data into BigQuery.
    """
    client = bigquery.Client()
    table_full_name = f"{project_id}.{dataset_id}.{table_id}"

    # Adding timestamp
    json_data = [
        {
            **data,
            "datetime_insercao": datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S"),
        }
        for data in json_data
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition=write_disposition,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="datetime_insercao",  # name of column to use for partitioning
        ),
        # clustering_fields=["timestamp_insercao"],
    )

    try:
        job = client.load_table_from_json(json_data, table_full_name, job_config=job_config)
        job.result()
    except Exception as e:
        raise Exception(e)


def build_redis_key(
    dataset_id: str, table_id: str, name: str = None, mode: Literal["dev", "prod"] = "prod"
) -> str:
    """
    Constructs a Redis key from a dataset ID, table ID and optional name.

    The key is constructed by concatenating the dataset ID and table ID with a
    dot (.) separator. If a name is provided, it is appended to the key. If the
    mode is "dev", it is prepended to the key with a dot separator.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        name (str, optional): The name of the Redis key. Defaults to None.
        mode (str, optional): The mode of the Redis key (prod or dev). Defaults to "prod".

    Returns:
        str: The constructed Redis key.
    """
    key = dataset_id + "." + table_id
    if name:
        key = key + "." + name
    if mode == "dev":
        key = f"{mode}.{key}"

    return key


def build_redis_name(
    dataset_id: str, table_id: str, name: str = None, mode: Literal["dev", "prod"] = "prod"
) -> str:
    """
    Constructs a Redis key from a dataset ID, table ID and optional name.

    The key is constructed by concatenating the dataset ID and table ID with a
    underscore (_) separator. If a name is provided, it is appended to the key. If the
    mode is "dev", it is prepended to the key with a dot separator.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        name (str, optional): The name of the Redis key. Defaults to None.
        mode (str, optional): The mode of the Redis key (prod or dev). Defaults to "prod".

    Returns:
        str: The constructed Redis key.
    """
    key = dataset_id + "_" + table_id
    if name:
        key = key + "_" + name
    if mode == "dev":
        key = f"{mode}_{key}"

    return key


def get_on_redis(
    dataset_id: str,
    table_id: str,
    name: str = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str = "oB8uffvoWCDBbff0x8RxCnHOXtS81Cx6",
) -> list:
    """
    Retrieves a list of values from Redis based on a given dataset ID, table ID
    and optional name. If the mode is "dev", it is prepended to the key with a
    dot separator.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        name (str, optional): The name of the Redis key. Defaults to None.
        mode (str, optional): The mode of the Redis key (prod or dev). Defaults to "prod".

    Returns:
        list: The list of values associated with the Redis key.
    """
    redis_client = get_redis_client(password=redis_password)

    key = build_redis_key(dataset_id, table_id, name, mode)
    files_on_redis = redis_client.get(key)
    return files_on_redis


def save_on_redis(
    data: any,
    dataset_id: str,
    table_id: str,
    name: str = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str = "oB8uffvoWCDBbff0x8RxCnHOXtS81Cx6",
) -> None:
    """
    Saves a given data to Redis based on a given dataset ID, table ID and
    optional name. If the mode is "dev", it is prepended to the key with a
    dot separator.

    Args:
        data (any): The data to be saved to Redis.
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        name (str, optional): The name of the Redis key. Defaults to None.
        mode (str, optional): The mode of the Redis key (prod or dev). Defaults to "prod".
    """
    redis_client = get_redis_client(password=redis_password)
    key = build_redis_key(dataset_id, table_id, name, mode)
    redis_client.hset(name=key, value=data)


def h_save_on_redis(
    data: any,
    dataset_id: str,
    table_id: str,
    name: str = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str = "oB8uffvoWCDBbff0x8RxCnHOXtS81Cx6",
) -> None:
    """
    Saves a given data to Redis based on a given dataset ID, table ID and
    optional name. If the mode is "dev", it is prepended to the key with a
    dot separator.

    Args:
        data (any): The data to be saved to Redis.
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        name (str, optional): The name of the Redis key. Defaults to None.
        mode (str, optional): The mode of the Redis key (prod or dev). Defaults to "prod".
    """
    redis_client = get_redis_client(password=redis_password)
    key = build_redis_key(dataset_id, table_id, name, mode)
    redis_client.hset(name=key, mapping=data)


def ts_get_on_redis(
    dataset_id: str,
    table_id: str,
    name: str = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str = "oB8uffvoWCDBbff0x8RxCnHOXtS81Cx6",
) -> list:
    """
    Retrieves a list of values from Redis based on a given dataset ID, table ID
    and optional name. If the mode is "dev", it is prepended to the key with a
    dot separator.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        name (str, optional): The name of the Redis key. Defaults to None.
        mode (str, optional): The mode of the Redis key (prod or dev). Defaults to "prod".

    Returns:
        list: The list of values associated with the Redis key.
    """
    redis_client = get_redis_client(password=redis_password)

    key = build_redis_key(dataset_id, table_id, name, mode)
    files_on_redis = redis_client.get_with_timestamp(key)
    return files_on_redis


def h_get_on_redis(
    dataset_id: str,
    table_id: str,
    name: str = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str = "oB8uffvoWCDBbff0x8RxCnHOXtS81Cx6",
) -> list:
    """
    Retrieves a list of values from Redis based on a given dataset ID, table ID
    and optional name. If the mode is "dev", it is prepended to the key with a
    dot separator.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        name (str, optional): The name of the Redis key. Defaults to None.
        mode (str, optional): The mode of the Redis key (prod or dev). Defaults to "prod".

    Returns:
        list: The list of values associated with the Redis key.
    """
    redis_client = get_redis_client(password=redis_password)

    key = build_redis_key(dataset_id, table_id, name, mode)
    files_on_redis = redis_client.hgetall(name=key)
    return files_on_redis


def binary_to_base_64_string(file_path):
    """
    Converts a binary file to a Base64-encoded string.

    Args:
        file_path (str): The path to the binary file.

    Returns:
        str: The Base64-encoded string.
    """
    with open(file_path, "rb") as file:
        binary_data = file.read()

    binary_string = base64.b64encode(binary_data).decode("utf-8")

    return binary_string


def base64_to_file(base64_string, file_path):
    """
    Converts a Base64-encoded string to a binary file and saves it to a given file path.

    Args:
        base64_string (str): The Base64-encoded string.
        file_path (str): The path to the file to be saved.

    Returns:
        None
    """
    binary_data = base64.b64decode(base64_string)

    session_file = file_path

    with open(session_file, "wb") as file:
        file.write(binary_data)


async def get_new_telegram_messages(
    client: TelegramClient, channel_name: str, redis_last_dates: Dict[str, datetime] = None
) -> int:
    """
    Asynchronously fetches new messages from a Telegram channel and stores them in a Channel object.

    Args:
        client (TelegramClient): The Telegram client to fetch messages.
        channel_name (str): The name of the channel to fetch messages from.
        redis_last_dates (Dict[str, datetime], optional): A dictionary containing the last dates
        of messages fetched from each channel. Defaults to None.

    Returns:
        int: The number of new messages fetched from the channel.
    """
    log(msg=f"Fetching messages from {channel_name}...")
    loop_start_date = redis_last_dates.get(channel_name, None) if redis_last_dates else None

    channel = Channel(channel_name)

    async for message in client.iter_messages(channel_name):
        if message.text and message.date:
            if (
                message.date.strftime("%Y-%m-%d %H:%M:%S") > loop_start_date
                if loop_start_date
                else True
            ):
                new_message = Message(
                    chat_id=str(message.chat.id) if message.chat else "",
                    chat_username=message.chat.username if message.chat else "",
                    chat_name=message.chat.title if message.chat else "",
                    message_id=str(message.id),
                    timestamp=message.date.strftime("%Y-%m-%d %H:%M:%S"),
                    text=message.text,
                    media=message.media is not None,
                    views=str(message.views) if message.views else "",
                    geolocalizacao=str(message.geo) if message.geo else "",
                )
                channel.add_message(new_message)
            else:
                break

    messages_qty = channel.len_mensagens()

    if messages_qty > 0:
        log(f"Found {messages_qty} new messages in {channel_name}...")
    else:
        log(f"No new messages in {channel_name}...")

    return messages_qty


async def get_telegram_client(
    api_id: str, api_hash: str, phone_number: str, session_name: str
) -> TelegramClient:
    """
    Asynchronously creates a TelegramClient instance and starts it with the given
    phone number.

    Args:
        api_id (str): The API ID of the Telegram account.
        api_hash (str): The API hash of the Telegram account.
        phone_number (str): The phone number associated with the Telegram account.
        session_name (str): The name of the session to use for this client.

    Returns:
        TelegramClient: The created Telegram client.
    """

    client = TelegramClient(session_name, api_id, api_hash)
    await client.start(phone_number)
    return client


def update_redis_last_dates(canais_dict: Dict[str, Channel], config: object):
    """
    Updates the last dates of messages in Redis for each channel provided.

    Iterates through the provided dictionary of channels, extracting the last
    message date from each channel and updating the Redis store with this
    information.

    Args:
        canais_dict (Dict[str, Channel]): A dictionary where keys are channel
            names and values are Channel objects.
        config (object): A configuration object containing Redis client and
            Redis name attributes.
    """
    redis_last_dates = {}

    for channel in canais_dict.values():
        if channel.last_date:
            redis_last_dates[channel.nome] = channel.last_date

    log("Updating last dates on redis...")
    config.redis_client.hset(name=config.redis_name, mapping=redis_last_dates)
