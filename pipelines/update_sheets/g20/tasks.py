# -*- coding: utf-8 -*-
"""
This module contains tasks for appending new data to Google Sheets.
"""
from datetime import datetime
from typing import List, Literal

import basedosdados as bd
import pandas as pd
import pytz
from infisical import InfisicalClient
from prefect import task

# from prefect.engine.runner import ENDRUN
# from prefect.engine.state import Skipped
from prefeitura_rio.pipelines_utils.infisical import get_secret_folder
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.update_sheets.g20.utils import (
    api_civitas_get_token,
    get_data_from_google_sheets,
    get_waze_police_alerts,
    send_data_to_google_sheets,
)

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True
tz = pytz.timezone("America/Sao_Paulo")


@task
def task_get_disque_denuncia_newest_data(spreadsheet_id: str, range_name: str) -> List[List]:

    values = get_data_from_google_sheets(spreadsheet_id=spreadsheet_id, range_name=range_name)

    df_sheets = pd.DataFrame(values, columns=["id_denuncia", "data_denuncia"])
    id_denuncias_hoje = df_sheets.loc[
        df_sheets["data_denuncia"] >= datetime.now(tz=tz).strftime("%Y-%m-%d")
    ]["id_denuncia"]

    ids_denuncias_list = id_denuncias_hoje.to_list()
    ids = "', '".join(map(str, ids_denuncias_list))

    query = rf"""
    SELECT
    a.id_denuncia,
    a.data_denuncia,
    a.data_difusao,
    CONCAT(
        COALESCE(a.tipo_logradouro, ''),
        COALESCE(CONCAT(' ', a.logradouro), ''),
        COALESCE(CONCAT(', ', a.numero_logradouro), ''),
        COALESCE(CONCAT(' - ', a.complemento_logradouro), ''),
        COALESCE(CONCAT(' - ', a.referencia_logradouro), '')
    ) AS local,
    a.bairro_logradouro,
    a.subbairro_logradouro,
    a.latitude,
    a.longitude,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT x.nome), ',\n') AS xptos,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CONCAT(o.id,' - ', o.nome, ' - ', o.tipo)), ',\n') AS orgaos,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT c.classe), ',\n') AS assuntos,
    ARRAY_TO_STRING(
        ARRAY_AGG(
        DISTINCT CONCAT(
            COALESCE(CONCAT('id: ', e.id), ''),
            COALESCE(CONCAT(', nome: ', e.nome), ''),
            COALESCE(CONCAT(', vulgo: ', e.vulgo), ''),
            COALESCE(CONCAT(', sexo: ', e.sexo), ''),
            COALESCE(CONCAT(', idade: ', e.idade), ''),
            COALESCE(CONCAT(', pele: ', e.pele), '')
            )
        ),
            ',\n'
        ) AS envolvidos,
    relato,
    timestamp_insercao
    FROM
    `rj-civitas.disque_denuncia.denuncias` a
    LEFT JOIN UNNEST(a.xptos) x
    LEFT JOIN UNNEST(a.orgaos) o
    LEFT JOIN UNNEST(a.assuntos) c
    LEFT JOIN UNNEST(a.envolvidos) e
    WHERE
    data_denuncia >= CURRENT_DATE('America/Sao_Paulo')
    AND id_denuncia NOT IN ('{ids}')
    GROUP BY ALL
    ORDER BY
    timestamp_insercao;
    """

    df = bd.read_sql(query)

    colunas = df.columns
    for coluna in colunas:
        df[coluna] = df[coluna].astype(str)

    # Substituindo NaN e None por strings vazias
    df.fillna("", inplace=True)

    final_values = df.values.tolist()
    return final_values


@task
def task_get_fogo_cruzado_newest_data(spreadsheet_id: str, range_name: str) -> List[List]:
    values = get_data_from_google_sheets(spreadsheet_id=spreadsheet_id, range_name=range_name)

    df_sheets = pd.DataFrame(values, columns=["id_ocorrencia"])
    ids_ocorrencias = df_sheets["id_ocorrencia"].to_list()
    ids = "', '".join(map(str, ids_ocorrencias))
    query = f"""SELECT
        *
    FROM
        `rj-civitas.fogo_cruzado.ocorrencias`
    WHERE
        timestamp_insercao >= CURRENT_DATE('America/Sao_Paulo')
        AND data_ocorrencia >= CURRENT_DATE('America/Sao_Paulo')
        AND id_ocorrencia NOT IN ('{ids}')
    ORDER BY timestamp_insercao;"""

    df = bd.read_sql(query)

    colunas = df.columns
    for coluna in colunas:
        df[coluna] = df[coluna].astype(str)

    # Substituindo NaN e None por strings vazias
    df.fillna("", inplace=True)

    final_values = df.values.tolist()

    return final_values


@task
def task_get_wave_police_newest_data(
    api_secrets: dict, spreadsheet_id: str, range_name: str
) -> List[List]:
    log("Getting timestamps from Google Sheets...")
    values = get_data_from_google_sheets(spreadsheet_id=spreadsheet_id, range_name=range_name)

    df_sheets = pd.DataFrame(values, columns=["timestamp"])
    max_timestamp = df_sheets["timestamp"].max()
    log(f"Max timestamp: {max_timestamp}")

    log("Authenticating to Waze API...")
    token = api_civitas_get_token(
        api_url=api_secrets.get("API_URL_BASE", None),
        user=api_secrets.get("USERNAME", None),
        password=api_secrets.get("PASSWORD", None),
    )

    log("Fetching data from Waze API...")
    df = get_waze_police_alerts(api_url=api_secrets.get("API_URL_BASE", None), token=token)

    log("Converting data types...")
    columns = df.columns
    for column in columns:
        df[column] = df[column].astype(str)

    # Substituindo NaN e None por strings vazias
    df.fillna("", inplace=True)

    log("Filtering data...")
    df_new = df.loc[df["timestamp"] > max_timestamp]

    log("Converting data to list...")
    final_values = df_new.values.tolist()

    return final_values


@task
def task_append_new_data_to_google_sheets(
    values: List[List], spreadsheet_id: str, range_name: str
) -> None:
    if values == []:
        log(f"{range_name} - Nenhum dado encontrado.")
        return None

    send_data_to_google_sheets(spreadsheet_id=spreadsheet_id, range_name=range_name, values=values)


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
