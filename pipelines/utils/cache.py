# -*- coding: utf-8 -*-
from typing import Literal

from redis_pal import RedisPal


def get_redis_client(
    host: str = "redis-master",
    port: int = 6379,
    db: int = 0,  # pylint: disable=C0103
    password: str = None,
) -> RedisPal:
    """
    Returns a Redis client.
    """
    return RedisPal(
        host=host,
        port=port,
        db=db,
        password=password,
    )


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


def get_on_redis(
    dataset_id: str,
    table_id: str,
    name: str = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str = None,
) -> dict:
    """
    Retrieves an object from Redis based on a given dataset ID, table ID
    and optional name. If the mode is "dev", it is prepended to the key with a
    dot separator.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        name (str, optional): The name of the Redis key. Defaults to None.
        mode (str, optional): The mode of the Redis key (prod or dev). Defaults to "prod".
        redis_password (str, optional): The password of the Redis server. Defaults to None.
    Returns:
        dict: The object associated with the Redis key.
    """
    redis_client = get_redis_client(password=redis_password)

    key = build_redis_key(dataset_id, table_id, name, mode)
    object_on_redis = redis_client.get(key)
    return object_on_redis


def save_on_redis(
    data: any,
    dataset_id: str,
    table_id: str,
    name: str = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str = None,
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
    redis_client.set(key, data)
