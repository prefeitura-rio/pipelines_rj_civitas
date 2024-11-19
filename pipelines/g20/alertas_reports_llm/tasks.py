# -*- coding: utf-8 -*-
"""
This module contains tasks for appending new data to Google Sheets.
"""
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from typing import Dict, List, Literal, Tuple

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
    """
    Returns the current date and time in the UTC timezone.
    """
    return datetime.now(tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S")


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
                datetime(current_timestamp()), interval {minutes_interval} minute
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
                TIMESTAMP_DIFF(
                        CURRENT_TIMESTAMP(),
                        data_report,
                        MINUTE
                    ) <= {minutes_interval}
        """
            if start_datetime is None or end_datetime is None
            else f"""
                datetime(data_report) BETWEEN '{start_datetime}' AND '{end_datetime}'
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
    log(f"Found {len(dataframe)} rows")

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
        bigquery.SchemaField(name="cidade_inteira_contexto", field_type="BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField(name="solicitantes_contexto", field_type="STRING", mode="REPEATED"),
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

        table_exists = check_if_table_exists(dataset_id=dataset_id, table_id=table_id)

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

            # wait some seconds after table creation
            if not table_exists and batch_index == 0:
                log("Waiting for table to be created...")
                time.sleep(10)

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
    minutes_interval: int = 360,
) -> pd.DataFrame:

    if not isinstance(minutes_interval, int) or minutes_interval < 0:
        raise ValueError("minutes_interval must be an integer greater than 0")

    query = rf"""WITH
  contexts_by_report AS (
  SELECT
    a.id_report,
    a.date_execution,
    a.relation,
    solicitante,
    a.cidade_inteira_contexto,
    ARRAY_AGG(a.id_relacao) AS id_relacao--,
  FROM
    `{project_id}.{dataset_id}.{table_id}` a
    JOIN UNNEST(a.solicitantes_contexto) AS solicitante
  WHERE
    a.relation = TRUE
    AND LOWER(a.threat_level_report) = 'alto'
    AND ARRAY_LENGTH(solicitantes_contexto ) > 0
  GROUP BY
    ALL
),
  alert_ids AS (
  SELECT
    a.*,
    CAST(
        FARM_FINGERPRINT(
            CONCAT(
                a.relation,
                ARRAY_TO_STRING(
                    ARRAY(
                        SELECT
                            element
                        FROM
                            UNNEST(id_relacao) AS element
                        ORDER BY
                            element ASC
                    ),
                    ', ' ),
              a.date_execution,
              a.solicitante,
              a.cidade_inteira_contexto
            )
        ) AS STRING
    ) AS id_alerta
  FROM
    contexts_by_report a
)
SELECT
  DISTINCT
  a.id_alerta,
  c.*,
  a.solicitante,
  a.cidade_inteira_contexto
FROM
  `{project_id}.{dataset_id}.{table_id}` c
JOIN
  (
    SELECT id_report, id, id_alerta, solicitante, cidade_inteira_contexto FROM alert_ids, UNNEST(id_relacao) AS id) a
ON
  a.id = c.id_relacao
WHERE
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), c.data_report, MINUTE) <= {minutes_interval}"""

    source_table_exists = check_if_table_exists(dataset_id=dataset_id, table_id=table_id)
    if not source_table_exists:
        log(f"Source table {dataset_id}.{table_id} does not exist.")
        return pd.DataFrame()

    destiny_table_exists = check_if_table_exists(
        dataset_id=dataset_id, table_id="alertas_historico"
    )

    if destiny_table_exists:
        query += f"\nAND NOT EXISTS (SELECT 1 FROM `{project_id}.{dataset_id}.alertas_historico` b WHERE b.id_alerta = a.id_alerta )"

    log("Searching for new alerts.")
    log(f"New alerts query: \n{query}")
    df = bd.read_sql(query)

    if len(df) == 0:
        log("There is no new alerts to be sent to Discord.")
        return pd.DataFrame()  # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    else:
        log(f"Found {len(df)} new alerts.")
        return df


# @task
# def task_get_new_alerts(
#     project_id: str,
#     dataset_id: str,
#     table_id: str,
#     date_execution: str,
# ) -> pd.DataFrame:
#     query = f"""
#     SELECT DISTINCT *
#     FROM `{project_id}.{dataset_id}.{table_id}`
#     WHERE date_execution = '{date_execution}'
#     AND relation = TRUE
#     """
#     log(f"Searching for new alerts with date_execution: {date_execution}")
#     df = bd.read_sql(query)

#     if len(df) == 0:
#         skip_flow_run("There is no new alerts to be sent to Discord.")
#     else:
#         log(f"Found {len(df)} new alerts.")
#         return df


@task
def task_build_messages_text(
    dataframe: pd.DataFrame,
) -> List:
    """
    Builds text messages for new alerts by processing and formatting alert data.

    This function takes a DataFrame generated by task_get_new_alerts() containing alert data,
    processes it to create formatted text messages with metadata and context information.
    It handles multiple records per alert, generates maps when coordinates are available,
    and processes everything in parallel for better performance.

    Args:
        dataframe (pd.DataFrame): DataFrame generated by task_get_new_alerts() containing the alert records

    Returns:
        List[Dict]: A list of dictionaries containing formatted messages. Each dictionary has:
            - meta: Dict with metadata like id_relacao, requester, whole_city flag
            - data: Dict with message text, map image data (if available), and processed contexts

    Example output format:
    [
        {
            "meta": {
                "id_relacao": "123",
                "solicitante": "AGENCY",
                "cidade_inteira": False
            },
            "data": {
                "message": "Formatted text...",
                "image_data": bytes or None
            }
        },
        ...
    ]
    """
    if len(dataframe) == 0:
        log("No new alerts to build messages text.")
        return []

    log("Building messages text for new alerts...")

    # Prepare DataFrame - agora agrupando por id_report, solicitante e cidade_inteira
    # df_exploded = dataframe.explode('solicitantes_contexto')
    # filtered_df = df_exploded.dropna(subset=['solicitantes_contexto'])
    filtered_df = dataframe.dropna(subset=["solicitante"])

    selected_df = filtered_df[
        [
            "id_alerta",
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
            "cidade_inteira_contexto",
            "solicitante",
        ]
    ]

    def process_single_record(
        record_group: Tuple[Tuple, List[Dict]],
        fix_bad_formatting=None,
        get_delay_time_string=None,
        generate_png_map=None,
    ) -> Dict:
        """
        Process a single group of records to create a formatted message.

        Args:
            record_group (Tuple[Tuple, List[Dict]]): Tuple containing (id_report, solicitante, cidade_inteira)
                                                    and list of records
        """
        group_key, records = record_group
        id_report, solicitante, cidade_inteira = group_key
        first_record = records[0]

        # Add metadata with new grouping
        metadata = {
            # "id_relacao": [record['id_relacao'] for record in records],
            "id_alerta": first_record["id_alerta"],
            "solicitante": solicitante.upper().strip(),
            "cidade_inteira": cidade_inteira,
        }

        # Build base message
        msg = f"""
## {fix_bad_formatting(first_record['title_report']) or 'Não Informado'}

**ID Report:** {id_report}
**Contextos:** {', '.join(sorted({r['nome_contexto'] for r in records}))}

- **Atraso:** {get_delay_time_string(first_record['data_report'])}
- **Fonte:** {first_record['id_source']}
- **ID:** {first_record['id_report_original']}
- **Data:** {first_record['data_report'].strftime("%d/%m/%Y %H:%M:%S")}
- **Endereço:** {first_record['logradouro_report']}, {first_record['numero_logradouro_report']}

- **Descrição:** {first_record['descricao_report'] or 'Não Informado'}

----
"""

        # Agrupar contextos por nome para evitar duplicatas
        contextos_por_nome = {}
        for ctx in records:
            nome_contexto = ctx["nome_contexto"].strip() or "Não Informado"
            if nome_contexto not in contextos_por_nome:
                contextos_por_nome[nome_contexto] = {
                    "id_relacao": ctx["id_relacao"],
                    "nome": nome_contexto,
                    "confianca": ctx["relation_confidence"],
                    "fatores": ctx["relation_key_factors"],
                    "data_inicio": ctx["datahora_inicio_contexto"],
                    "data_fim": ctx["datahora_fim_contexto"],
                }

        # Process unique contexts
        contextos_processados = []
        for idx, (nome, ctx) in enumerate(sorted(contextos_por_nome.items()), 1):
            contextos_processados.append(ctx)

            msg += f"""
**{idx}. {ctx['nome']}**
- **Data Início:** {ctx['data_inicio']}
- **Data Fim:** {ctx['data_fim']}
- **Confiança:** {ctx['confianca']}
- **ID Relação:** {ctx['id_relacao']}
- **Fatores:**
"""
            for fator in ctx["fatores"]:
                msg += f"  - {fix_bad_formatting(fator)}\n"

        # Generate map
        map_data = None
        if first_record["latitude_report"] and first_record["longitude_report"]:
            map_data = generate_png_map(
                [(first_record["latitude_report"], first_record["longitude_report"])]
            )

        return {
            "meta": metadata,
            "data": {
                "message": msg,
                "image_data": map_data,
            },
        }

    # Convert DataFrame to optimized format with new grouping
    records_dict = {}
    for record in selected_df.to_dict("records"):
        group_key = (record["id_report"], record["solicitante"], record["cidade_inteira_contexto"])
        records_dict.setdefault(group_key, []).append(record)

    # Create partial function with dependencies
    process_func = partial(
        process_single_record,
        fix_bad_formatting=fix_bad_formatting,
        get_delay_time_string=get_delay_time_string,
        generate_png_map=generate_png_map,
    )

    # Process in parallel
    start = time.time()
    with ThreadPoolExecutor(max_workers=4) as executor:
        messages = list(executor.map(process_func, records_dict.items()))

    end = time.time()
    log(f"Messages built and ready to be sent: {len(messages)} in {end-start:.2f} seconds")

    return messages


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_send_discord_messages(
    webhooks: dict[str, str], messages: list[dict], project_id: str, dataset_id: str, table_id: str
) -> None:
    """
    Send messages to different Discord channels and save alert history to BigQuery.

    Args:
        webhooks (Dict[str, str]): Dictionary mapping channel names to webhook URLs
        messages (List[Dict]): List of message dictionaries
        project_id (str): BigQuery project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID
    """
    # def generate_alert_id(data: dict) -> str:
    #     """
    #     Generate deterministic hash from data information
    #     """
    #     # Sort meta keys to ensure consistent ordering
    #     meta_str = json.dumps(data, sort_keys=True)
    #     return hashlib.sha256(meta_str.encode()).hexdigest()

    # def generate_alert_id(meta: dict) -> str:
    #     """
    #     Generate deterministic hash from meta information
    #     """
    #     # Sort meta keys to ensure consistent ordering
    #     meta_str = json.dumps(meta, sort_keys=True)

    #     query = fr"""SELECT
    # farm_fingerprint(
    #     CONCAT(
    #         {meta['cidade_inteira']},
    #         ARRAY_TO_STRING({sorted(meta['id_relacao'])}, ', '),
    #         "{meta['solicitante']}"
    #     )
    #     ) AS id_farmprint_json_string"""

    #     log(f'Query generate alert id:\n {query}')
    #     alert_id =bd.read_sql(query)

    #     return alert_id

    def save_alert_history(meta: dict, message_text: str, timestamp: datetime) -> None:
        """
        Save alert metadata to BigQuery history table
        """
        alert_data = {
            "id_alerta": meta["id_alerta"],
            "solicitante": meta["solicitante"],
            "cidade_inteira": meta["cidade_inteira"],
            "timestamp_alert": pd.Timestamp(timestamp),
            "texto_alerta": message_text,
        }

        # Convert to DataFrame for easy upload
        df = pd.DataFrame([alert_data])

        # Upload to BigQuery
        load_data_from_dataframe(
            dataframe=df,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            write_disposition="WRITE_APPEND",
            schema=[
                bigquery.SchemaField(name="id_alerta", field_type="STRING", mode="REQUIRED"),
                bigquery.SchemaField(name="solicitante", field_type="STRING", mode="REQUIRED"),
                bigquery.SchemaField(name="cidade_inteira", field_type="BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField(
                    name="timestamp_alert", field_type="TIMESTAMP", mode="REQUIRED"
                ),
                bigquery.SchemaField(name="texto_alerta", field_type="STRING", mode="REQUIRED"),
            ],
        )

    async def main():
        if not messages:
            log("No messages to send.")
            return None

        def split_by_newline(text: str, limit: int = 2000) -> list[str]:
            chunks = []
            while text:
                if len(text) <= limit:
                    chunks.append(text)
                    break

                split_index = text[:limit].rfind("\n")
                if split_index == -1:
                    split_index = limit

                chunks.append(text[:split_index])
                text = text[split_index:]

            return chunks

        def get_webhook_url(meta: dict) -> str:
            webhook_key = (
                f"G20_{meta['solicitante']}_{'CIDADE' if meta['cidade_inteira'] else 'CONTEXTOS'}"
            )
            return webhooks.get(webhook_key)

        log("Starting to send messages to Discord.")
        for message_obj in messages:
            log(
                f"Sending message {message_obj['meta']['id_alerta']} to Discord: {message_obj['meta']['solicitante']}"
            )
            webhook_url = get_webhook_url(message_obj["meta"])
            if not webhook_url:
                log(f"No webhook URL found for meta: {message_obj['meta']}")
                continue

            message_data = message_obj["data"]
            chunks = split_by_newline(message_data["message"])

            # Send text chunks sequentially without image
            for i in range(len(chunks) - 1):
                await send_discord_message(
                    webhook_url=webhook_url, message=chunks[i], image_data=None
                )

            # Send the last chunk with image
            await send_discord_message(
                webhook_url=webhook_url, message=chunks[-1], image_data=message_data["image_data"]
            )

            # Save alert history after successful send
            timestamp_utc = datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")
            save_alert_history(message_obj["meta"], message_data["message"], timestamp_utc)

        log("Messages sent to discord and history saved successfully.")

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
