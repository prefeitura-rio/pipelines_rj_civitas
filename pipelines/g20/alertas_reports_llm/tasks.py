# -*- coding: utf-8 -*-
"""
This module contains tasks for appending new data to Google Sheets.
"""
import asyncio

# import time
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
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.g20.alertas_reports_llm.model import (
    EnrichResponseModel,
    Model,
    RelationResponseModel,
)
from pipelines.g20.alertas_reports_llm.utils import (  # ml_generate_text,; query_data_from_sql_file,
    check_if_table_exists,
    fix_bad_formatting,
    get_delay_time_string,
    load_data_from_dataframe,
)
from pipelines.utils import send_discord_message

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True
tz = pytz.timezone("America/Sao_Paulo")


@task
def task_get_date_execution() -> str:
    return datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")


@task
def task_get_data(
    project_id: str,
    dataset_id: str,
    source: str,
    table_id: str,
    query_template: str,
    prompt: str,
    start_datetime: str = None,
    end_datetime: str = None,
    minutes_interval: int = 30,
    date_execution: str = None,
) -> pd.DataFrame:
    if source not in ["enriquecimento", "relacao"]:
        raise ValueError("source deve ser 'enriquecimento' ou 'relacao'")

    if source == "enriquecimento":
        try:
            minutes_interval = int(minutes_interval)
        except Exception as e:
            raise ValueError(f"{e} - minutes_interval must be an integer")

        date_filter = (
            f"""
                datetime(data_report) >= timestamp_sub(
                datetime(current_timestamp(), 'America/Sao_Paulo'), interval {minutes_interval} minute
                )
        """
            if start_datetime is None or end_datetime is None
            else f"""
                datetime(data_report)  BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        )
    else:
        date_filter = (
            f"""
                date_execution = '{date_execution}'
        """
            if start_datetime is None or end_datetime is None
            else f"""
                datetime(date_execution) BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        )

    table_exists = check_if_table_exists(dataset_id=dataset_id, table_id=table_id)

    id_column = "id_enriquecimento" if source == "enriquecimento" else "id_relacao"
    select_replacer = (
        f"""
        select
            p.*
        from prompt_id p
        left join `{project_id}.{dataset_id}.{table_id}` e on p.{id_column} = e.{id_column}
        where e.id_report is null
    """
        if table_exists
        else """
        select
            *
        from prompt_id
    """
    )

    query = (
        query_template.replace("__prompt_replacer__", prompt)
        .replace("__date_filter_replacer__", date_filter)
        .replace("__final_select_replacer__", select_replacer)
    )

    log(f"Query {source.capitalize()}:\n\n{query}")
    dataframe = bd.read_sql(query)

    return dataframe.reset_index()


@task
def task_get_llm_reponse_and_update_table(
    dataframe: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    prompt_column: str = None,
    model_name: str = "gemini-1.5-flash",
    max_output_tokens: int = 1024,
    temperature: float = 0.2,
    top_k: int = 32,
    top_p: int = 1,
    project_id: str = "rj-civitas",
    location: str = "us-central1",
    batch_size: int = 10,
    date_execution: str = None,
) -> None:

    if len(dataframe) > 0:

        log(f"Start generate llm response for {prompt_column} with {len(dataframe)} rows")
        if prompt_column == "prompt_enriquecimento":
            response_schema = EnrichResponseModel.schema()
        elif prompt_column == "prompt_relacao":
            response_schema = RelationResponseModel.schema()
        else:
            raise ValueError("prompt_column must be 'prompt_enriquecimento' or 'prompt_relacao'")

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
            for prompt, index in zip(dataframe[prompt_column].tolist(), dataframe["index"].tolist())
        ]
        model = Model()
        model.vertex_init(project_id=project_id, location=location)

        def chunks(input_list: list, batch_size: int):
            for i in range(0, len(input_list), batch_size):
                yield input_list[i : i + batch_size]  # noqa

        for batch_index, batch in enumerate(chunks(model_input, batch_size)):
            log(f"Processing batch {batch_index + 1}/{(len(model_input) // batch_size + 1)}")

            responses = model.model_predict_batch(model_input=batch)

            batch_df = dataframe.merge(pd.DataFrame(responses), on="index")
            batch_df = batch_df.drop(columns=["index"])
            batch_df["date_execution"] = pd.Timestamp(date_execution)

            batch_df["error_name"] = batch_df["error_name"].astype(str)
            batch_df["error_message"] = batch_df["error_message"].astype(str)

            load_data_from_dataframe(
                dataframe=batch_df, project_id=project_id, dataset_id=dataset_id, table_id=table_id
            )

    else:
        log(f"No new data to load to {dataset_id}.{table_id}")


# @task
def skip_flow_run(
    message: str,
) -> None:
    """
    Skips the flow run by raising a Skipped state with the provided message.

    Args:
        message: Message to include in the Skipped state

    Raises:
        ENDRUN: Raises a Skipped state to terminate flow execution
    """
    skip = Skipped(message=message)
    log(message)
    raise ENDRUN(state=skip)


@task
def task_get_new_alerts(
    project_id: str,
    dataset_id: str,
    table_id: str,
    date_execution: str,
) -> pd.DataFrame:
    query = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE date_execution = '{date_execution}'
    """
    log(f"Searching for new alerts with date_execution: {date_execution}")
    df = bd.read_sql(query)

    if len(df) == 0:
        skip_flow_run("There is no new alerts to be sent to Discord.")
    else:
        return df


@task
def task_build_messages_text(
    df: pd.DataFrame,
) -> List:
    log("Building messages text for new alerts...")
    filtered_df = df.loc[df["relation"]]  # Column with Boolean type

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

        relation_key_factors = [
            f"  - {fix_bad_formatting(factor)}  " for factor in occurrence["relation_key_factors"]
        ]
        relation_key_factors_str = "\n".join(relation_key_factors)
        message = (
            f"**RELATÓRIO G20**\n\n"
            f"- **Contexto**: {occurrence['nome_contexto']}\n"
            f"- **Atraso**: {get_delay_time_string(occurrence, 'data_report')}\n"
            f"- **Fonte**: {occurrence['id_source']}\n"
            f"- **ID**: {occurrence['id_report_original']}\n"
            f"- **Data**: {occurrence['data_report']}\n\n"
            f"- **Descrição**: {occurrence['descricao_report']}\n"
            f"- **Motivo da Relação**: {fix_bad_formatting(occurrence['relation_explanation'])}\n"
            f"- **Confiança**: {occurrence['relation_confidence']}\n"
            f"- **Fatores da Relação**: \n{relation_key_factors_str}\n"
        )

        messages.append(message)

    return messages


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_send_discord_messages(url_webhook: str, messages: List[str], image_data=None) -> None:
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
