# -*- coding: utf-8 -*-
"""
This module contains tasks for the alerts pipeline.
"""

import asyncio
import os
from datetime import timedelta
from typing import List, Literal

import basedosdados as bd
import pandas as pd
from prefect import task
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Skipped
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode

from pipelines.utils.cache import get_on_redis, save_on_redis
from pipelines.utils.notifications import get_delay_time_string, send_discord_message

bd.config.from_file = True

MAX_DATETIME_REPORT = ""


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_get_new_reports(
    project_id: str,
    dataset_id: str,
    table_id: str,
    start_datetime: str = None,
    keywords: List[str] = [],
    mode: Literal["dev", "staging", "prod"] | None = None,
):
    """
    Retrieves new reports from the database based on the provided parameters.

    Args:
        project_id (str): The ID of the BigQuery project.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.
        start_datetime (datetime): The start datetime to retrieve reports from (GTM-3).
        keywords (List[str]): The keywords to filter reports by.
        mode (Literal["dev", "staging", "prod"] | None): The mode to run the flow in.
    Returns:
        pd.DataFrame: A DataFrame containing the new reports.
    """
    global MAX_DATETIME_REPORT

    log("Getting flow run mode...", level="info")
    if mode is None:
        mode = get_flow_run_mode()

    log("Getting last report datetime from Redis...", level="info")
    redis_last_report_datetime = get_on_redis(
        dataset_id=dataset_id,
        table_id=table_id,
        name="last_report_datetime",
        mode=mode,
        redis_password=os.getenv("REDIS_PASSWORD"),
    )

    if redis_last_report_datetime:
        log(f"Last report datetime found on Redis: {redis_last_report_datetime}", level="info")
    else:
        log("No last report datetime found on Redis, using start datetime...", level="info")

    log("Building query to get new reports from BigQuery...", level="info")
    query = f"""
    SELECT DISTINCT
        id_denuncia,
        data_denuncia,
        data_difusao,
        timestamp_insercao,
        relato
    FROM
        {project_id}.{dataset_id}.{table_id}
    WHERE
        data_denuncia >= '{redis_last_report_datetime or start_datetime}'
    """

    if keywords:
        keywords_lower = [keyword.lower() for keyword in keywords]
        query += f" AND REGEXP_CONTAINS(LOWER(relato), r'({'|'.join(keywords_lower)})')"

    query += " ORDER BY data_denuncia DESC"

    log(f"Query: {query}", level="info")
    log("Executing query...", level="info")
    reports_response = bd.read_sql(query, billing_project_id=project_id)

    if reports_response.empty:
        log("There is no new reports, finishing the flow.", level="info")
        skip = Skipped(message="There is no new reports, finishing the flow.")
        raise ENDRUN(state=skip)

    else:
        MAX_DATETIME_REPORT = reports_response["timestamp_insercao"].max()
        log(f"New reports found, returning {len(reports_response)} reports...", level="info")
        return reports_response


@task
def task_create_messages(reports_response: pd.DataFrame):
    """
    Creates messages for the Discord alerts based on the reports response.

    Args:
        reports_response (pd.DataFrame): The reports response.

    Returns:
        pass
    """
    messages = []
    log("Creating messages for the Discord alerts...", level="info")
    for index, row in reports_response.iterrows():
        atraso = get_delay_time_string(df=row, column_name="data_denuncia")
        message = f"**Data da Denúncia:** {row['data_denuncia']}\n"
        message += f"**ID da Denúncia:** {row['id_denuncia']}\n"
        message += f"**Atraso:** {atraso}\n\n"
        message += f"**Relato:** {row['relato']}\n"

        messages.append(message)

    log(f"Created {len(messages)} messages for the Discord alerts...", level="info")
    return messages


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_send_discord_messages(messages: List[str]):
    """
    Sends messages to the Discord channel.

    Args:
        messages (List[str]): The messages to send.
    """
    log("Getting Discord webhook URL...", level="info")
    webhook_url = os.getenv("DD_CASO_CDD_WEBHOOK_URL")

    async def main():
        log("Sending messages to the Discord channel...", level="info")
        for message in messages:
            log(f"Sending message to the Discord channel: {message}", level="info")
            await send_discord_message(
                webhook_url=webhook_url, message=message, username="Disque Denúncia"
            )

    asyncio.run(main())


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_update_last_report_datetime(
    dataset_id: str, table_id: str, mode: Literal["dev", "staging", "prod"] | None = None
):
    """
    Saves the last report datetime on Redis.
    """
    global MAX_DATETIME_REPORT

    mode = mode or get_flow_run_mode()

    log("Saving last report datetime on Redis...", level="info")
    save_on_redis(
        data=MAX_DATETIME_REPORT,
        dataset_id=dataset_id,
        table_id=table_id,
        name="last_report_datetime",
        mode=mode,
        redis_password=os.getenv("REDIS_PASSWORD"),
    )
    log(f"Last report datetime saved on Redis: {MAX_DATETIME_REPORT}", level="info")
