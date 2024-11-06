# -*- coding: utf-8 -*-
# import json
# import concurrent.futures
import re
import time
from datetime import datetime

import basedosdados as bd
import pandas as pd
import pytz

# import os
import vertexai
from google.api_core import exceptions
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.io import get_root_path
from prefeitura_rio.pipelines_utils.logging import log
from vertexai.generative_models import GenerativeModel  # GenerationConfig,

tz = pytz.timezone("America/Sao_Paulo")


def fix_bad_formatting(text: str) -> str:
    return re.sub(r"\n+", "", text)


def get_delay_time_string(occurrence_timestamp):
    """
    Returns a string with the time difference between the current datetime and the datetime
    in the 'data_ocorrencia' column of the given dataframe.

    Args:
        df_ocorrencias (pd.DataFrame): The dataframe with the 'data_ocorrencia' column.

    Returns:
        str: A string with the time difference (e.g. "3 dias, 2 horas, 1 minuto e 2 segundos").
    """

    delta = datetime.now(tz=tz) - occurrence_timestamp

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


def query_data_from_sql_file(model_dataset_id: str, model_table_id: str) -> pd.DataFrame:
    log(f"Querying data from {model_dataset_id}.{model_table_id}")
    root_path = get_root_path()
    model_path = root_path / f"queries/models/{model_dataset_id}/{model_table_id}.sql"
    query = model_path.read_text(encoding="utf-8")
    # query = f"""SELECT * FROM {dataset_id}.{table_id}"""

    df = bd.read_sql(query)

    return df


def check_if_table_exists(dataset_id: str, table_id: str) -> bool:
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    return tb.table_exists(mode="prod")


def load_data_from_dataframe(
    dataframe: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    project_id: str = None,
    write_disposition: str = "WRITE_APPEND",
    schema: list[bigquery.SchemaField] = [],
) -> None:
    client = bigquery.Client()
    destination_table = ""
    destination_table += f"{project_id}." if project_id else ""
    destination_table += f"{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=schema or None,
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition=write_disposition,
        # time_partitioning=bigquery.TimePartitioning(
        #     type_=bigquery.TimePartitioningType.MONTH,
        #     field="timestamp_insercao",  # name of column to use for partitioning
        # ),
        # clustering_fields=["timestamp_insercao"],
    )

    client.load_table_from_dataframe(
        dataframe, destination=destination_table, num_retries=3, job_config=job_config
    )


# def ml_generate_text(project_id: str = None, location: str = "us-central1",
# model_name: str = "gemini-1.5-flash", prompt: str = None) -> str:
#     vertexai.init(project=project_id, location=location)
#     model = GenerativeModel(model_name)
#     generation_config = GenerationConfig(
#         temperature=0.2,
#     )
#     response = model.generate_content(prompt, generation_config)
#     return response.text


def generate_content(model: GenerativeModel, prompt: str) -> str:
    # Chame a função de geração de conteúdo de forma síncrona
    response = model.generate_content(prompt)
    response_text = ""
    for candidate in response.candidates:
        for part in candidate.content.parts:
            response_text += part.text
        finish_reason = str(candidate.finish_reason)
    return (response_text, finish_reason)


def safe_generate_content(
    model_name: str,
    # model: GenerativeModel,
    prompt: str,
) -> str:

    vertexai.init(location="us-central1")
    model = GenerativeModel(model_name)

    for attempt in range(5):  # Tente até 5 vezes
        try:
            return generate_content(model, prompt)
        except exceptions.ResourceExhausted:
            time.sleep(10**attempt)  # Aumenta o tempo de espera a cada tentativa
    return None  # Se falhar, retorne None ou trate de outra forma


# def ml_generate_text(df: pd.DataFrame, prompt: str,
# model_name: str = "gemini-1.5-flash") -> list[str]:
#     antes = datetime.now()
#     # Inicializa o modelo
#     vertexai.init(location="us-central1")
#     model = GenerativeModel(model_name)

#     final_responses = []
#     prompts = []

#     COUNTER = 0
#     for _, row in df.iterrows():
#         if COUNTER >= 10:
#             break

#         # prompt = '''Dado este texto sobre uma ocorrência, determine:
#         #     1. O nível de abrangência apropriado (Casa, Quadra, Bairro, Região da Cidade, Cidade
# , Estado, País)
#         #     2. O nível de urgência (Nenhuma, Baixa, Média, Alta) da ocorrência que descreve o
# potencial de escalada do problema caso não seja tratado. A urgência está relacionada com o nível de risco à vida das pessoas ao redor.
#         #     3. Prever horários de início e fim com base na data do relatório e no desenrolar pre
# visto do contexto.
#         #     Texto:
#         #     ID da Ocorrência: {id_report}
#         #     ID da Fonte: {id_source},
#         #     ID Original: {id_report_original},
#         #     Data do Relatório (Quando a denúncia chegou à prefeitura): {data_report},
#         #     Categoria: {categoria},
#         #     Tipo/Subtipo: {tipo_subtipo},
#         #     Descrição: {descricao},
#         #     Organizações: {orgaos},
#         #     Endereço: {logradouro}, {numero_logradouro},
#         #     Localização: {latitude}, {longitude},
#         #     Retorne apenas os seguintes campos em JSON:
#         #     {{
#         #     "id_report": "{id_report}",
#         #     "scope_level_explanation": "Explicação para o nível de abrangência",
#         #     "scope_level": "nível de abrangência",
#         #     "urgency_explanation": "Explicação para o nível de urgência",
#         #     "urgency": "nível de urgência",
#         #     "predicted_times_explanation": "Explicação para as datas previstas",
#         #     "predicted_start_time": "timestamp ISO format" (%Y-%m-%d %H:%M:%S),
#         #     "predicted_end_time": "timestamp ISO format (%Y-%m-%d %H:%M:%S)",
#         #     }}
#         #     RETORNE APENAS O JSON, SEM EXPLICAÇÕES. SE HOUVER ASPAS OU CARACTERES ESPECIAIS
# DENTRO DOS TEXTOS, INSIRA O ESCAPE.
#         # '''.format(
#         #     id_report=row['id_report'],
#         #     id_source=row['id_source'],
#         #     id_report_original=row['id_report_original'],
#         #     data_report=row['data_report'],
#         #     categoria=row['categoria'],
#         #     tipo_subtipo=row['tipo_subtipo'],
#         #     descricao=row['descricao'],
#         #     orgaos=row['orgaos'],
#         #     logradouro=row['logradouro'],
#         #     numero_logradouro=row['numero_logradouro'],
#         #     latitude=row['latitude'],
#         #     longitude=row['longitude'],
#         # )
#         prompts.append((model, prompt))
#         COUNTER += 1
#         # print(prompt)

#     # Execute as tarefas em paralelo
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         futures = [executor.submit(safe_generate_content, model, prompt) for model,
# prompt in prompts]

#         for future in concurrent.futures.as_completed(futures):
#             response = future.result()
#             final_responses.append(response)
#             # print(response)  # Exiba a resposta

#     # response = model.generate_content(prompt)
#     # response_text = ''
#     # for candidate in responses.candidates:
#     #   for part in candidate.content.parts:
#     #     response_text += part.text

#     depois = datetime.now()

#     log (f">>>>>>>>>> Tempo total: {depois - antes}\n")
#     log(final_responses[0])
#     return final_responses
