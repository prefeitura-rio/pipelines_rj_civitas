# -*- coding: utf-8 -*-
"""
This module contains tasks for appending new data to Google Sheets.
"""
import asyncio
import concurrent.futures
import json

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
from prefeitura_rio.pipelines_utils.io import get_root_path
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.g20.dbt_run_relatorio_enriquecido.utils import (  # ml_generate_text,; query_data_from_sql_file,
    check_if_table_exists,
    get_delay_time_string,
    load_data_from_dataframe,
    safe_generate_content,
)
from pipelines.utils import send_discord_message

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True
tz = pytz.timezone("America/Sao_Paulo")


# use bd.read_sql to get the table integrations from
# rj-civitas.integracao_reports.reports
@task
def task_get_occurrences(
    dataset_id: str, table_id: str, minutes_interval: int = 30
) -> pd.DataFrame:
    try:
        minutes_interval = int(minutes_interval)
    except Exception as e:
        raise ValueError(f"{e} - minutes_interval must be an integer")

    query = rf"""select
            ifnull(id_report, '') as id_report,
            ifnull(id_source, '') as id_source,
            ifnull(id_report_original, '') as id_report_original,
            ifnull(
                datetime(data_report, 'America/Sao_Paulo'), cast('' as datetime)
            ) as data_report,
            ifnull(
                array(select ifnull(orgao, '') from unnest(orgaos) as orgao), []
            ) as orgaos,
            ifnull(categoria, '') as categoria,
            array(
                select
                    struct(
                        ifnull(item.tipo, '') as tipo,
                        ifnull(
                            array(
                                select ifnull(sub, '') from unnest(item.subtipo) as sub
                            ),
                            []
                        ) as subtipo
                    )
                from unnest(tipo_subtipo) as item
            ) as tipo_subtipo,
            ifnull(descricao, '') as descricao,
            ifnull(logradouro, '') as logradouro,
            ifnull(numero_logradouro, '') as numero_logradouro,
            ifnull(latitude, cast(0 as float64)) as latitude,
            ifnull(longitude, cast(0 as float64)) as longitude
        from `rj-civitas.{dataset_id}.{table_id}` tablesample system(10 percent)
        where
            datetime(data_report, 'America/Sao_Paulo') >= timestamp_sub(
                datetime(current_timestamp(), 'America/Sao_Paulo'), interval {minutes_interval} minute
            )
            and id_source = 'DD'"""

    df = bd.read_sql(query)

    return df


@task
def task_check_if_table_exists(dataset_id: str, table_id: str) -> bool:
    return check_if_table_exists(dataset_id, table_id)


@task
def task_update_dados_enriquecidos_table(
    df: pd.DataFrame, dataset_id: str, table_id: str, model_name: str = "gemini-1.5-flash"
) -> None:
    target_table_exists = check_if_table_exists(
        dataset_id=dataset_id,
        table_id=table_id,
    )

    log(f">>>>>>>>>>>>>> {target_table_exists}")

    if len(df) > 0:
        log(f"Creating {len(df)} prompts...")
        df["prompt"] = df.apply(
            lambda row: """
            Você é um analista de segurança especializado no evento do G20.
            Sua função é avaliar ocorrências e classificar o risco potencial para a vida e integridade
            física dos participantes.
            Avalie o risco independentemente da localização exata do evento ou ocorrência.
            Forneça justificativas claras e objetivas para cada classificação, e preencha todos os campos
            do JSON com precisão.
            Siga as instruções passo a passo.

            Para cada ocorrência, siga as instruções abaixo:

            1. **Tópicos**:
                - Identifique o tópico principal e quaisquer tópicos relacionados com base na descrição da ocorrência.
                - Exemplos de tópicos principais incluem: “ameaça à segurança”, “condições climáticas adversas”,
                “protestos”, “problemas de infraestrutura”, se necessário, adicione tópicos relacionados
                para complementar a classificação.
                - Justifique a escolha do tópico principal e dos relacionados com base na descrição do evento.

            2. **Nível de Abrangência**:
                - Classifique o nível de impacto com uma justificativa detalhada, usando uma das
                categorias a seguir:
                    - **Casa**: Afeta apenas uma residência/estabelecimento
                    - **Quadra**: Impacto limitado à área da quadra
                    - **Bairro**: Afeta um bairro inteiro
                    - **Região**: Impacta múltiplos bairros
                    - **Cidade**: Afeta toda a cidade
                    - **Estado**: Impacto em nível estadual
                    - **País**: Repercussão nacional
                - Descreva o motivo da escolha do nível de abrangência com base no escopo potencial de impacto.

            3. **Avaliação Temporal**:
                - Defina o intervalo de tempo em minutos até o possível início da ocorrência,
                explicando a estimativa com base nos dados disponíveis.
                - Use “0” para tempos indefinidos.
                - Explique como chegou à estimativa para o horário previsto, considerando as informações
                fornecidas.

            4. **Nível de Ameaça**:
                - Avalie o risco e escolha entre os níveis abaixo:
                    - **BAIXO**: Risco indireto ou muito improvável (não representa risco direto à
                    vida ou integridade, inclui maus-tratos a animais, transgressões ambientais,
                    trabalhistas, etc.)
                    - **ALTO**: Ameaça iminente à vida ou integridade física (inclui tiroteios,
                    bloqueio de vias, manifestações, ameaças de bombas ou terrorismo).
                - Justifique a avaliação da ameaça com uma análise objetiva do risco envolvido,
                considerando o potencial de dano à vida e integridade física dos participantes.

            Ocorrencia :
            ID da Ocorrência: {id_report}

            Data do Relatório (Quando a denúncia chegou à prefeitura): cast({data_report} as string)
            Categoria: {categoria}
            Tipo/Subtipo: to_json_string({tipo_subtipo})

            Organizações: to_json_string({orgaos})
            Descrição: {descricao}

            Retorne apenas os seguintes campos em JSON nomeados exatamente como abaixo:
            {{
                "id_report": "{id_report}",
                "main_topic": "tópico principal",
                "related_topics": ["array de tópicos relacionados"],
                "scope_level_explanation": "Explicação para o nível de abrangência",
                "scope_level": "nível de abrangência",
                "predicted_time_explanation": "Explicação para os horários previstos",
                "predicted_time_interval": "valor horário previsto",
                "threat_explanation": "Avaliação detalhada da ameaça",
                "threat_level": "valor nível de ameaça"
            }}
            **Instrução adicional**: Ao retornar os dados em JSON, **escape** corretamente todos os
            caracteres especiais, como aspas duplas (`"`), barras invertidas (`\\`), traços (`-`),
            e outros caracteres que podem afetar a sintaxe do JSON.
            **Não altere** a acentuação ou os caracteres normais da língua portuguesa.
            """.format(
                id_report=row["id_report"],
                data_report=row["data_report"],
                categoria=row["categoria"],
                tipo_subtipo=row["tipo_subtipo"],
                orgaos=row["orgaos"],
                descricao=row["descricao"],
            ),
            axis=1,
        )

        if target_table_exists:
            query = f"""SELECT id_report, prompt FROM `rj-civitas.{dataset_id}.{table_id}`"""
            old_ids = bd.read_sql(query)

            data = df.loc[
                (~df["id_report"].isin(old_ids["id_report"]))
                & (~df["prompt"].isin(old_ids["prompt"]))
            ]
        else:
            data = df

        if len(data) > 0:
            log(f"Loading {len(data)} rows to {dataset_id}.{table_id}")

            antes = datetime.now()
            final_responses = []
            finish_reasons = []
            prompts = [(model_name, row["prompt"]) for _, row in data.iterrows()]

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(safe_generate_content, model, prompt)
                    for model, prompt in prompts
                ]

                for future in concurrent.futures.as_completed(futures):
                    response = future.result()
                    final_responses.append(response[0])
                    finish_reasons.append(response[1])

            # load_data_from_dataframe(data, dataset_id, table_id)
            # ml_generate_text(data, final_responses, prompts)
            depois = datetime.now()

            log(f">>>>>>>>>> Tempo total: {depois - antes}\n")
            # log(final_responses[0])
            # final_responses

            dados_dict = []
            finish_reasons_list = []
            for json_string, finish_reason in zip(final_responses, finish_reasons):
                try:
                    if "```json" in json_string:
                        # Remover a marcação e extrair a parte JSON
                        json_part = json_string.split("```json")[1].split("```")[0].strip()

                        # Converte a string JSON em um dicionário
                        dados_dict.append(json.loads(json_part))
                        finish_reasons_list.append(finish_reason)
                    else:
                        # log(f" JSON STRING BUGADA >>>>>>>>: {json_string}")
                        dados_dict.append(json.loads(json_string))
                        finish_reasons_list.append(finish_reason)

                except (IndexError, json.JSONDecodeError) as e:
                    log(f"Error processing json string: {json_string}\nError: {e}")

            # log(dados_dict[0])
            enriched_data = pd.DataFrame(dados_dict)
            enriched_data["finish_reason"] = finish_reasons_list

            final_df = pd.merge(data, enriched_data, on="id_report", how="left")

            load_data_from_dataframe(final_df, dataset_id, table_id)
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
