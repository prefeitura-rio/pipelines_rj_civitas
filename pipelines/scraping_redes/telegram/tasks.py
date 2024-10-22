# -*- coding: utf-8 -*-
import asyncio
import traceback
from datetime import timedelta
from typing import Literal

import pandas as pd
import pytz
from google.cloud import bigquery
from infisical import InfisicalClient
from prefect import task
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Failed, Skipped
from prefeitura_rio.pipelines_utils.infisical import get_secret_folder
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.scraping_redes.telegram.models.pipeline_config import Pipeline
from pipelines.scraping_redes.telegram.models.telegram import Channel
from pipelines.scraping_redes.telegram.utils import (
    get_new_telegram_messages,
    get_telegram_client,
    save_data_in_bq,
)

tz = pytz.timezone("America/Sao_Paulo")


@task
def task_create_pipeline(**kwargs):

    Pipeline.initialize(
        project_id=kwargs["project_id"],
        dataset_id=kwargs["dataset_id"],
        table_id=kwargs["table_id"],
        mode=kwargs["mode"],
        channels_names=kwargs["channels_names"],
        redis_secrets=kwargs["redis_secrets"],
        telegram_secrets=kwargs["telegram_secrets"],
    )


async def fetch_new_messages_from_telegram_channels():
    log("Getting Telegram Client...")
    client = await get_telegram_client(
        api_id=Pipeline.telegram_secrets.get("API_ID"),
        api_hash=Pipeline.telegram_secrets.get("API_HASH"),
        phone_number=Pipeline.telegram_secrets.get("API_PHONE_NUMBER"),
        session_name=constants.SESSION_NAME.value,
    )

    log("Getting channels last dates from Redis...")
    redis_last_dates = Pipeline.channels_last_dates

    log("Fetching new messages from channels...")
    tasks = []
    async with client:
        for channel in Pipeline.channels_names:
            tasks.append(
                asyncio.create_task(
                    get_new_telegram_messages(
                        client=client, channel_name=channel, redis_last_dates=redis_last_dates
                    )
                )
            )

        messages_quantities = await asyncio.gather(*tasks)

    if sum(messages_quantities) == 0:
        log("There is no new messages from any channel. Exiting...")
        skip = Skipped(message="No data returned by the API, finishing the flow.")
        raise ENDRUN(state=skip)


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_load_to_bigquery():
    messages_list = Channel.to_dict_list()

    schema = [
        bigquery.SchemaField(name="chat_id", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="chat_username", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="chat_name", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="message_id", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="timestamp", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="media", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="views", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="geolocalizacao", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="datetime_insercao", field_type="DATETIME", mode="NULLABLE"),
    ]

    try:
        save_data_in_bq(
            project_id=Pipeline.project_id,
            dataset_id=Pipeline.dataset_id,
            table_id=Pipeline.table_id,
            schema=schema,
            json_data=messages_list,
            write_disposition="WRITE_TRUNCATE",
        )

        # Updating redis last dates
        df = pd.DataFrame(messages_list)
        max_dates = df.groupby("chat_username")["timestamp"].max().reset_index()
        max_dates_dict = max_dates.set_index("chat_username")["timestamp"].to_dict()
        Pipeline.update_channels_last_dates(max_dates_dict)

    except Exception as e:
        # Capture the traceback
        tb_str = traceback.format_exc()
        fail = Failed(
            message=f"Error while saving data to BigQuery: {e}" + "\nTraceback:\n" + tb_str
        )
        log(fail.message)
        raise ENDRUN(state=fail)


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_fetch_new_messages_from_telegram_channels():
    asyncio.run(fetch_new_messages_from_telegram_channels())


@task
def task_get_secret_folder(
    secret_path: str = "/",
    secret_name: str = None,
    type: Literal["shared", "personal"] = "personal",
    environment: str = None,
    client: InfisicalClient = None,
) -> dict:
    """
    Fetches secrets from Infisical. If passing only `secret_path` and
    no `secret_name`, returns all secrets inside a folder.

    Args:
        secret_name (str, optional): _description_. Defaults to None.
        secret_path (str, optional): _description_. Defaults to '/'.
        environment (str, optional): _description_. Defaults to 'dev'.

    Returns:
        _type_: _description_
    """
    secrets = get_secret_folder(
        secret_path=secret_path,
        secret_name=secret_name,
        type=type,
        environment=environment,
        client=client,
    )
    return secrets
