# -*- coding: utf-8 -*-
# import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
import pytz
import requests
from google.cloud import bigquery
from redis_pal import RedisPal

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

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition=write_disposition,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="timestamp_insercao",  # name of column to use for partitioning
        ),
        clustering_fields=["timestamp_insercao"],
    )

    # Adding timestamp
    json_data = [
        {
            **data,
            "timestamp_insercao": datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S"),
        }
        for data in json_data
    ]

    try:
        job = client.load_table_from_json(json_data, table_full_name, job_config=job_config)
        job.result()
    except Exception as e:
        raise Exception(e)


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


def safe_float_conversion(str_value):

    if isinstance(str_value, float):
        return str_value

    # Check how many negative signs are present
    negative_sign_count = str_value.count("-")

    if negative_sign_count > 1:
        # Remove all but one negative sign
        str_value = str_value.replace("-", "", negative_sign_count - 1)

    try:
        return float(str_value)
    except ValueError:
        # Return None or a default value if conversion fails
        return None


def is_token_valid(token_data: Optional[Dict[str, Any]]) -> bool:
    """
    Checks if the API token is still valid.

    Args:
        token_data: Dictionary containing 'accessToken' and 'expiresAt'

    Returns:
        bool: True if token exists and is still valid, False otherwise
    """
    if not token_data:
        return False

    access_token = token_data.get("accessToken")
    expires_at_str = token_data.get("expiresAt")

    if not all([access_token, expires_at_str]):
        return False

    try:
        expires_at = datetime.strptime(expires_at_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        current_time = datetime.now(tz=timezone.utc)
        return expires_at > current_time
    except ValueError as e:
        raise Exception(f"Error parsing expiration date: {e}")


def update_token_on_redis(data: requests.Response, redis_password: str) -> None:
    """
    Updates the token on Redis with the new expiration date.

    Args:
        data (requests.Response): The response from the /auth/login endpoint.
        redis_password (str): The password for the Redis connection.
    """
    request_date_str: str = data.headers.get("Date")
    request_date_obj: datetime = pd.to_datetime(request_date_str)
    request_date_obj

    expires_at: datetime = request_date_obj + timedelta(
        seconds=data.json().get("data", {}).get("expiresIn", 0)
    )
    expires_at_str: str = expires_at.strftime("%Y-%m-%d %H:%M:%S")

    data: dict = data.json().get("data", {})
    data.update({"expiresAt": expires_at_str})

    save_on_redis(
        dataset_id="fogo_cruzado",
        table_id="ocorrencias",
        name="api_token",
        data=data,
        redis_password=redis_password,
    )
