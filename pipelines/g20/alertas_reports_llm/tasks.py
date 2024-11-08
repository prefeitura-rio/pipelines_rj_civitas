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
from google.cloud import bigquery
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
from pipelines.utils import generate_png_map, send_discord_message

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
    table_id_enriquecido: str = "",
    query_template: str = None,
    prompt: str = None,
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
                datetime(date_execution) = '{date_execution}'
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
        .replace("__project_id__", project_id)
        .replace("__dataset_id__", dataset_id)
        .replace("__table_id_enriquecido__", table_id_enriquecido)
    )

    log(f"Query {source.capitalize()}:\n\n{query}")
    dataframe = bd.read_sql(query)

    return dataframe.reset_index()


def get_default_value_for_field(field: bigquery.SchemaField, length: int):
    if field.mode == "REPEATED":
        return [[] for _ in range(length)]

    defaults = {
        "STRING": ["" for _ in range(length)],
        "INTEGER": [0 for _ in range(length)],
        "INT64": [0 for _ in range(length)],
        "FLOAT": [0.0 for _ in range(length)],
        "FLOAT64": [0.0 for _ in range(length)],
        "BOOLEAN": [False for _ in range(length)],
        "BOOL": [False for _ in range(length)],
        "TIMESTAMP": [None for _ in range(length)],
        "DATETIME": [None for _ in range(length)],
        "DATE": [None for _ in range(length)],
        "STRUCT": [None for _ in range(length)],
    }
    return defaults.get(field.field_type, [None for _ in range(length)])


def get_bq_table_schema(source: str = None) -> list[bigquery.SchemaField]:
    enriquecimento = [
        bigquery.SchemaField(name="id_enriquecimento", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="id_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="id_source", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="id_report_original", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_report", field_type="TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField(name="orgaos", field_type="STRING", mode="REPEATED"),
        bigquery.SchemaField(name="categoria", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="tipo_subtipo",
            field_type="STRUCT",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(name="subtipo", field_type="STRING", mode="REPEATED"),
                bigquery.SchemaField(name="tipo", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(name="descricao", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="logradouro", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="numero_logradouro", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="latitude", field_type="FLOAT", mode="NULLABLE"),
        bigquery.SchemaField(name="longitude", field_type="FLOAT", mode="NULLABLE"),
        bigquery.SchemaField(name="prompt_enriquecimento", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="main_topic", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="related_topics", field_type="STRING", mode="REPEATED"),
        bigquery.SchemaField(name="scope_level_explanation", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="scope_level", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="predicted_time_explanation", field_type="STRING", mode="NULLABLE"
        ),
        bigquery.SchemaField(name="predicted_time_interval", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="threat_explanation", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="threat_level", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="finish_reason", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="error_name", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="error_message", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="date_execution", field_type="TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField(name="title_report", field_type="STRING", mode="NULLABLE"),
    ]
    relacao = [
        bigquery.SchemaField(name="id_relacao", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="id_report", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="id_source", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="id_enriquecimento", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="id_report_original", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="data_report", field_type="TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField(name="orgaos_report", field_type="STRING", mode="REPEATED"),
        bigquery.SchemaField(name="categoria_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="tipo_subtipo_report",
            field_type="STRUCT",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(name="subtipo", field_type="STRING", mode="REPEATED"),
                bigquery.SchemaField(name="tipo", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(name="descricao_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="logradouro_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="numero_logradouro_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="latitude_report", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="longitude_report", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="main_topic_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="related_topics_report", field_type="STRING", mode="REPEATED"),
        bigquery.SchemaField(name="scope_level_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="scope_level_explanation_report", field_type="STRING", mode="NULLABLE"
        ),
        bigquery.SchemaField(name="threat_level_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="threat_explanation_report", field_type="STRING", mode="NULLABLE"
        ),
        bigquery.SchemaField(name="title_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="predicted_time_interval_report", field_type="STRING", mode="NULLABLE"
        ),
        bigquery.SchemaField(
            name="predicted_end_time_report", field_type="TIMESTAMP", mode="NULLABLE"
        ),
        bigquery.SchemaField(
            name="predicted_time_explanation_report", field_type="STRING", mode="NULLABLE"
        ),
        bigquery.SchemaField(name="date_execution", field_type="TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField(name="id_contexto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="tipo_contexto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="datahora_inicio_contexto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="datahora_fim_contexto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="nome_contexto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="descricao_contexto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="informacoes_adicionais_contexto", field_type="STRING", mode="NULLABLE"
        ),
        bigquery.SchemaField(name="endereco_contexto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="local_contexto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="geometria_contexto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="raio_de_busca_contexto", field_type="INT64", mode="NULLABLE"),
        bigquery.SchemaField(name="data_report_tz", field_type="TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField(name="data_inicio_tz", field_type="TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField(name="prompt_relacao", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="relation_explanation", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="relation_key_factors", field_type="STRING", mode="REPEATED"),
        bigquery.SchemaField(name="relation_confidence", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="relation", field_type="BOOL", mode="NULLABLE"),
        bigquery.SchemaField(name="finish_reason", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="error_name", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="error_message", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="relation_title", field_type="STRING", mode="NULLABLE"),
    ]

    schema = relacao if "relacao" in source else enriquecimento

    return schema


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

        schema = get_bq_table_schema(source=prompt_column)

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

            schema_columns = {field.name: field for field in schema}
            missing_columns = set(schema_columns.keys()) - set(batch_df.columns)

            for col_name in missing_columns:
                field = schema_columns[col_name]
                default_value = get_default_value_for_field(field, len(batch_df))
                batch_df[col_name] = default_value

            load_data_from_dataframe(
                dataframe=batch_df,
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                schema=schema,
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
    SELECT DISTINCT *
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE date_execution = '{date_execution}'
    AND relation = TRUE
    """
    log(f"Searching for new alerts with date_execution: {date_execution}")
    df = bd.read_sql(query)

    if len(df) == 0:
        skip_flow_run("There is no new alerts to be sent to Discord.")
    else:
        return df


@task
def task_build_messages_text(
    dataframe: pd.DataFrame,
) -> List:
    log("Building messages text for new alerts...")
    selected_df = dataframe[
        [
            "id_report_original",
            "id_report",
            "id_source",
            "data_report",
            "descricao_report",
            "tipo_contexto",
            "nome_contexto",
            "relation_explanation",
            "relation_key_factors",
            "relation_confidence",
            "id_relacao",
            "latitude_report",
            "longitude_report",
            "geometria_contexto",
            "datahora_inicio_contexto",
            "datahora_fim_contexto",
            "logradouro_report",
            "numero_logradouro_report",
            "endereco_contexto",
            "title_report",
            "relation_title",
        ]
    ]

    messages = []
    for id_report_original in selected_df["id_report_original"].unique():
        msg = ""

        df_report = selected_df[selected_df["id_report_original"] == id_report_original]

        contextos = ", ".join(df_report["nome_contexto"])
        data_report = df_report["data_report"].unique()[0]
        atraso_report = get_delay_time_string(occurrence_timestamp=data_report)
        fonte_report = df_report["id_source"].unique()[0]
        data_report_str = data_report.strftime("%d/%m/%Y %H:%M:%S")
        descricao_report = df_report["descricao_report"].unique()[0]
        descricao_report = "Não Informado" if descricao_report == "" else descricao_report
        logradouro_report = df_report["logradouro_report"].unique()[0]
        numero_logradouro_report = df_report["numero_logradouro_report"].unique()[0]
        latitude_report = df_report["latitude_report"].unique()[0]
        longitude_report = df_report["longitude_report"].unique()[0]
        title_report = fix_bad_formatting(df_report["title_report"].unique()[0])
        title_report = "Não Informado" if title_report.strip() == "" else title_report.strip()

        endereco_report = f"{logradouro_report}, {numero_logradouro_report}"

        contextos_list = df_report[
            [
                "nome_contexto",
                "relation_confidence",
                "relation_key_factors",
                "id_relacao",
                "relation_explanation",
                "datahora_inicio_contexto",
                "datahora_fim_contexto",
                "endereco_contexto",
                "relation_title",
            ]
        ].to_dict("records")

        msg += f"""
## {title_report}

**Contextos:** {contextos}

- **Atraso:** {atraso_report}
- **Fonte:** {fonte_report}
- **ID:** {id_report_original}
- **Data:** {data_report_str}
- **Endereço:** {endereco_report}

- **Descrição:** {descricao_report}

----
"""

        for index, contexto in enumerate(contextos_list):
            nome_contexto = contexto["nome_contexto"].strip()
            nome_contexto = "Não Informado" if nome_contexto == "" else nome_contexto

            endereco_contexto = contexto["endereco_contexto"].strip()
            endereco_contexto = "Não Informado" if endereco_contexto == "" else endereco_contexto

            # relation_title = fix_bad_formatting(contexto["relation_title"])
            # relation_title = (
            #     "Não Informado" if relation_title.strip() == "" else relation_title.strip()
            # )

            msg += f"""
**{index+1}. {nome_contexto}**
- **Data Início:** {contexto['datahora_inicio_contexto']}
- **Data Fim:** {contexto['datahora_fim_contexto']}
- **Confiança:** {contexto['relation_confidence']}
- **Fatores:**
"""
            for fator in contexto["relation_key_factors"]:
                msg += f"""  - {fix_bad_formatting(fator)}\n"""
            # msg+=f"- **Descrição Relação:** {fix_bad_formatting(contexto['relation_explanation'])}\n"
            msg += f"- **ID:** {contexto['id_relacao']}\n"

        # plot map if there is latitude and longitude
        if latitude_report and longitude_report:
            map = generate_png_map(locations=[(latitude_report, longitude_report)])
        else:
            map = None

        messages.append(
            {
                "message": msg,
                "image_data": map,
            }
        )
    log(f"Messages built and ready to be sent: {len(messages)}")
    return messages


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_send_discord_messages(url_webhook: str, messages_contents: list[dict[str, bytes]]) -> None:
    """
    Send a list of messages to Discord using the given webhook URL.

    If the message is longer than 2000 characters, it is split into multiple messages.

    Args:
        url_webhook (str): The URL of the webhook.
        messages (List[str]): The list of messages to send.
        image_data (bytes, optional): The PNG image data to embed. Defaults to None.
    """

    async def main():
        if not messages_contents:
            log("No messages to send.")
            return None

        def split_by_newline(text: str, limit: int = 2000) -> list[str]:
            chunks = []
            while text:
                if len(text) <= limit:
                    chunks.append(text)
                    break

                # Find the last newline before the limit
                split_index = text[:limit].rfind("\n")
                if split_index == -1:  # If no newline found, use the limit
                    split_index = limit

                chunks.append(text[:split_index])
                text = text[split_index:]

            return chunks

        log("Start sending messages to discord.")
        for content in messages_contents:
            chunks = split_by_newline(content["message"])

            # Send text chunks sequentially without image
            for i in range(len(chunks) - 1):
                await send_discord_message(
                    webhook_url=url_webhook, message=chunks[i], image_data=None
                )

            # Send the last chunk with image
            await send_discord_message(
                webhook_url=url_webhook, message=chunks[-1], image_data=content["image_data"]
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
