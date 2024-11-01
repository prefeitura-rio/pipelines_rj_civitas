# -*- coding: utf-8 -*-
"""
This module contains tasks for appending new data to Google Sheets.
"""
import asyncio
from datetime import datetime, timedelta
from typing import List, Literal

import basedosdados as bd
import pandas as pd
import pytz
from infisical import InfisicalClient
from prefect import task
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Skipped
from prefeitura_rio.pipelines_utils.infisical import get_secret_folder
from prefeitura_rio.pipelines_utils.io import get_root_path
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils import send_discord_message

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True
tz = pytz.timezone("America/Sao_Paulo")


@task
def task_query_data_from_sql_file(model_dataset_id: str, model_table_id: str) -> pd.DataFrame:
    log(f"Querying data from {model_dataset_id}.{model_table_id}")
    root_path = get_root_path()
    model_path = root_path / f"queries/models/{model_dataset_id}/{model_table_id}.sql"
    query = model_path.read_text(encoding="utf-8")
    df = bd.read_sql(query)

    return df


@task
def task_skip_flow_run(data):
    if len(data) == 0:
        message = "There is no data to be sent to Discord."
        log(message)
        skip = Skipped(message=message)
        raise ENDRUN(state=skip)


def get_delay_time_string(df_ocorrencias: pd.DataFrame, datetime_column: str):
    """
    Returns a string with the time difference between the current datetime and the datetime
    in the 'data_ocorrencia' column of the given dataframe.

    Args:
        df_ocorrencias (pd.DataFrame): The dataframe with the 'data_ocorrencia' column.

    Returns:
        str: A string with the time difference (e.g. "3 dias, 2 horas, 1 minuto e 2 segundos").
    """
    occurrence_datetime = df_ocorrencias[datetime_column]
    delta = datetime.now(tz=tz) - occurrence_datetime.tz_localize(tz)

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


@task
def task_build_messages_text(df: pd.DataFrame) -> List:
    log("Building messages text...")
    filtered_df = df.loc[df["relation"].str.lower() == "true"]

    selected_df = filtered_df[
        [
            "id_report_original",
            "id_source",
            "data_report",
            "descricao_report",
            "tipo_contexto",
            "nome_contexto",
            "relation_explanation",
            "relation_key_factors",
            "relation_confidence",
        ]
    ]

    messages = []

    for _, occurrence in selected_df.iterrows():

        relation_key_factors = [f"  - {factor}  " for factor in occurrence["relation_key_factors"]]
        relation_key_factors_str = "\n".join(relation_key_factors)
        message = (
            f"**RELATÓRIO G20**\n\n"
            f"- **Contexto**: {occurrence['nome_contexto']}\n"
            f"- **Atraso**: {get_delay_time_string(occurrence, 'data_report')}\n"
            f"- **Fonte**: {occurrence['id_source']}\n"
            f"- **ID**: {occurrence['id_report_original']}\n"
            f"- **Data**: {occurrence['data_report']}\n\n"
            f"- **Descrição**: {occurrence['descricao_report']}\n"
            f"- **Motivo da Relação**: {occurrence['relation_explanation']}\n"
            f"- **Confiança**: {occurrence['relation_confidence']}\n"
            f"- **Fatores da Relação**: \n{relation_key_factors_str}\n"
        )

        messages.append(message)

    return messages


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_send_discord_messages(url_webhook: str, messages: List[str], image_data=None):
    """
    Send a list of messages to Discord using the given webhook URL.

    If the message is longer than 2000 characters, it is split into multiple messages.

    Args:
        url_webhook (str): The URL of the webhook.
        messages (List[str]): The list of messages to send.
        image_data (bytes, optional): The PNG image data to embed. Defaults to None.
    """

    async def main():
        if not messages:
            log("No messages to send.")
            return None

        log("Start sending messages to discord.")
        for message in messages:
            iteracoes_msg = len(message) // 2000

            for i in range(iteracoes_msg + 1):
                start_index = i * 2000
                end_index = (i + 1) * 2000
                await send_discord_message(
                    webhook_url=url_webhook,
                    message=message[start_index:end_index],
                    image_data=image_data,
                )

        log("Messages sent to discord successfully.")

    asyncio.run(main())


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
