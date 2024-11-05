# -*- coding: utf-8 -*-
"""
This module contains tasks for appending new data to Google Sheets.
"""
import asyncio

# import time
from datetime import timedelta
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

from pipelines.g20.dbt_run_relatorio_enriquecido.model import EnrichResponseModel, Model
from pipelines.g20.dbt_run_relatorio_enriquecido.utils import (  # ml_generate_text,; query_data_from_sql_file,
    check_if_table_exists,
    get_delay_time_string,
    load_data_from_dataframe,
)
from pipelines.utils import send_discord_message

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True
tz = pytz.timezone("America/Sao_Paulo")


# use bd.read_sql to get the table integrations from
# rj-civitas.integracao_reports.reports
@task
def task_get_occurrences(
    project_id: str,
    dataset_id: str,
    table_id: str,
    query_enriquecimento: str,
    prompt_enriquecimento: str,
    start_datetime: str = None,
    end_datetime: str = None,
    minutes_interval: int = 30,
) -> pd.DataFrame:
    try:
        minutes_interval = int(minutes_interval)
    except Exception as e:
        raise ValueError(f"{e} - minutes_interval must be an integer")

    if start_datetime is None or end_datetime is None:
        date_filter = f"""
                datetime(data_report, 'America/Sao_Paulo') >= timestamp_sub(
                datetime(current_timestamp(), 'America/Sao_Paulo'), interval {minutes_interval} minute
                )
        """
    else:
        date_filter = f"""
                datetime(data_report, 'America/Sao_Paulo')  BETWEEN {start_datetime} AND {end_datetime}
        """

    table_exists = check_if_table_exists(dataset_id=dataset_id, table_id=table_id)

    if table_exists:
        select_replacer = f"""
        select p.*
        from prompt_id p
        left join `{project_id}.{dataset_id}.{table_id}` e on p.id = e.id
        where e.id_report is null
        """
    else:
        select_replacer = """
        select
            *
        from prompt_id
            """

    query = (
        query_enriquecimento.replace("__prompt_replacer__", prompt_enriquecimento)
        .replace("__date_filter_replacer__", date_filter)
        .replace("__final_select_replacer__", select_replacer)
    )

    log(f"Query: {query}")
    dataframe = bd.read_sql(query)

    return dataframe


@task
def task_update_dados_enriquecidos_table(
    dataframe: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    model_name: str = "gemini-1.5-flash",
    max_output_tokens: int = 1024,
    temperature: float = 0.2,
    top_k: int = 32,
    top_p: int = 1,
    project_id: str = "rj-civitas",
    location: str = "us-central1",
    batch_size: int = 10,
) -> None:

    if len(dataframe) > 0:
        response_schema = EnrichResponseModel.schema()
        model_input = [
            {
                "prompt_text": prompt,
                "response_schema": response_schema,
                "model_name": model_name,
                "max_output_tokens": max_output_tokens,
                "temperature": temperature,
                "top_k": top_k,
                "top_p": top_p,
                "index": index,
            }
            for prompt, index in zip(
                dataframe["prompt_enriquecimento"].tolist(), dataframe["index"].tolist()
            )
        ]
        model = Model()
        model.vertex_init(project_id=project_id, location=location)

        def chunks(input_list: list, batch_size: int):
            for i in range(0, len(input_list), batch_size):
                yield input_list[i : i + batch_size]  # noqa

        for batch_index, batch in enumerate(chunks(model_input, batch_size)):
            log(f"Processing batch {batch_index + 1}/{(len(model_input) // batch_size)}")

            responses = model.model_predict_batch(model_input=batch)

            batch_df = dataframe.merge(pd.DataFrame(responses), on="index")
            load_data_from_dataframe(batch_df, dataset_id, table_id)

    else:
        log(f"No new data to load to {dataset_id}.{table_id}")


@task
def task_query_data_from_sql_file(
    model_dataset_id: str, model_table_id: str, minutes_ago: int = 30
) -> pd.DataFrame:
    log(f"Querying data from {model_dataset_id}.{model_table_id}")

    try:
        minutes_ago = int(minutes_ago)
    except Exception as e:
        raise ValueError(f"{e} - minutes_ago must be an integer")

    root_path = get_root_path()
    model_path = root_path / f"queries/models/{model_dataset_id}/{model_table_id}.sql"
    query = model_path.read_text(encoding="utf-8")
    final_query = query.replace("interval 30 minute", f"interval {minutes_ago} minute")
    # query = f"""SELECT * FROM {dataset_id}.{table_id}"""

    df = bd.read_sql(final_query)

    return df


# @task
# def filter_new_data(df: pd.DataFrame, dataset_id: str, table_id: str) -> pd.DataFrame:
#     query = f"""SELECT id FROM {dataset_id}.{table_id}"""
#     path = root_path / f"queries/models/{dataset_id}/{table_id}.sql"
#     query = model_path.read_text(encoding="utf-8")
#     # query = f"""SELECT * FROM {dataset_id}.{table_id}"""

#     df = bd.read_sql(query)

#     return df
# query = f""""""


@task
def task_skip_flow_run(data):
    if len(data) == 0:
        message = "There is no data to be sent to Discord."
        log(message)
        skip = Skipped(message=message)
        raise ENDRUN(state=skip)


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
