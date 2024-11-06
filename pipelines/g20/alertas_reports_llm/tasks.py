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
        bigquery.SchemaField(name="latitude_report", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="longitude_report", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="main_topic_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="scope_level_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="scope_level_explanation_report", field_type="STRING", mode="NULLABLE"
        ),
        bigquery.SchemaField(name="threat_level_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="threat_explanation_report", field_type="STRING", mode="NULLABLE"
        ),
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
    ]
    enriquecimento = [
        bigquery.SchemaField(name="id_enriquecimento", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="id_report", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="id_source", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="id_report_original", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_report", field_type="TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField(name="orgaos", field_type="STRING", mode="REPEATED"),
        bigquery.SchemaField(name="categoria", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="tipo_subtipo_report",
            field_type="STRUCT",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(name="subtipo", field_type="STRING", mode="REPEATED"),
                bigquery.SchemaField(name="tipo", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(name="subtipo", field_type="STRING", mode="REPEATED"),
        bigquery.SchemaField(name="tipo", field_type="STRING", mode="NULLABLE"),
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
    dataframe: pd.DataFrame,
) -> List:
    log("Building messages text for new alerts...")
    filtered_df = dataframe.loc[dataframe["relation"]]  # Column with Boolean type

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
            "id_relacao",
            "latitude_report",
            "longitude_report",
            "geometria_contexto",
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
        data_report_str = data_report.strftime("%Y-%m-%d %H:%M:%S")
        descricao_report = df_report["descricao_report"].unique()[0]

        contextos_list = df_report[
            ["nome_contexto", "relation_confidence", "relation_key_factors", "id_relacao"]
        ].to_dict("records")

        msg += f"""
**Contextos:** {contextos}

- **Atraso Ocorrência:** {atraso_report}
- **Fonte Ocorrência:** {fonte_report}
- **ID Ocorrência:** {id_report_original}
- **Data Ocorrência:** {data_report_str}

- **Descrição Ocorrência:** {descricao_report}

----
"""

        for contexto in contextos_list:
            nome_contexto = contexto["nome_contexto"].strip()
            nome_contexto = "Não Informado" if nome_contexto == "" else nome_contexto
            msg += f"""
**{nome_contexto}**
- **ID Relação:** {contexto['id_relacao']}
- **Confiança:** {contexto['relation_confidence']}
- **Fatores da Relação:**
"""
            for fator in contexto["relation_key_factors"]:
                msg += f"""  - {fix_bad_formatting(fator)}\n"""

        messages.append(msg)
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
