# -*- coding: utf-8 -*-
"""
This module contains tasks for retrieving, processing, and saving XML reports.

Tasks include:
- Retrieving reports from a REST API
- Saving reports as XML files
- Parsing and normalizing XML data
- Transforming XML data into structured CSV files
"""


from datetime import timedelta

import requests
import urllib3
from prefect import task
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Skipped
from prefeitura_rio.pipelines_utils.logging import log, log_mod
from pytz import timezone

from pipelines.fogo_cruzado.extract_load.utils import save_data_in_bq

tz = timezone("America/Sao_Paulo")
# Disable the warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def auth(email: str, password: str) -> str:
    """
    Authenticate with the Fogo Cruzado API to obtain an access token.

    Parameters
    ----------
    email : str
        The email address to use for authentication.
    password : str
        The password to use for authentication.

    Returns
    -------
    str
        The access token obtained from the Fogo Cruzado API.
    """
    host = "https://api-service.fogocruzado.org.br/api/v2"
    endpoint = "/auth/login"
    payload = {"email": email, "password": password}
    headers = {"Content-Type": "application/json"}

    response = requests.post(host + endpoint, json=payload, headers=headers, verify=False)
    response.raise_for_status()
    data = response.json()
    return data["data"]["accessToken"]


def get_ocorrencias(
    token: str,
    initial_date: str,
    take: int = 1000,
    id_state: str = "b112ffbe-17b3-4ad0-8f2a-2038745d1d14",
) -> list[dict]:

    """
    Fetches occurrences from the Fogo Cruzado API.

    Parameters
    ----------
    token : str
        The access token to use for authentication.
    initial_date : str
        The initial date to fetch occurrences from.
    take : int, optional
        The number of occurrences to fetch per page. Defaults to 1000.
    id_state : str, optional
        The ID of the state to fetch occurrences from.
        Defaults to "b112ffbe-17b3-4ad0-8f2a-2038745d1d14" [Rio de Janeiro, RJ]

    Returns
    -------
    list
        A list of dictionaries containing the occurrence data.
    """
    ocorrencias = []
    headers = {"Authorization": f"Bearer {token}"}
    params = {"initialdate": initial_date, "idState": id_state, "take": take}
    base_url = "https://api-service.fogocruzado.org.br/api/v2/occurrences?page={page}"

    # Primeira requisição para obter o número total de páginas
    initial_url = base_url.format(page=1)
    log_mod(msg="Loop 1: Getting data from API.", level="info", mod=100)
    response = requests.get(initial_url, headers=headers, params=params)
    response.raise_for_status()
    initial_data = response.json()
    total_pages = initial_data["pageMeta"]["pageCount"]
    ocorrencias.extend(initial_data["data"])

    # Requisições subsequentes para as demais páginas
    for page in range(2, total_pages + 1):
        url = base_url.format(page=page)
        log_mod(msg=f"Loop {page}: Getting data from API.", level="info", mod=100)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        ocorrencias.extend(data["data"])

    return ocorrencias


@task(max_retries=5, retry_delay=timedelta(seconds=30))
def fetch_ocorrencias(email: str, password: str, initial_date: str) -> list[dict]:
    """
    Task that Fetches occurrences from the Fogo Cruzado API.

    Parameters
    ----------
    email : str
        The email to use for authentication.
    password : str
        The password to use for authentication.
    initial_date : str
        The initial date to fetch occurrences from.

    Returns
    -------
    list
        A list of dictionaries containing the occurrence data.
    """

    token = auth(email=email, password=password)
    ocorrencias = get_ocorrencias(token=token, initial_date=initial_date)

    log("Fetching ocorrencias...")
    return ocorrencias
    # # if len(ocorrencias) == 0:
    #     # raise ValueError("Ocorrencias is None")  # SKIP FLOW

    # df_ocorrencias = pd.DataFrame(ocorrencias)

    # ########## -------------- APAGAR AQUI -------------- ##########
    # csv_dir = Path.cwd() / 'pipelines' / 'fogo_cruzado' /'data' / 'raw'
    # # df_ocorrencias = pd.read_csv(csv_dir / 'ocorrencias.csv')
    # csv_dir.mkdir(parents=True, exist_ok=True)
    # df_ocorrencias.to_csv(csv_dir / 'ocorrencias.csv', index=False)
    # ########## -------------- APAGAR AQUI -------------- ##########

    # ########## -------------- OPÇÃO 01 - TABELA NATIVA -------------- ##########
    # ########## -------------- OPÇÃO 01 - TABELA NATIVA -------------- ##########

    # ########## -------------- OPÇÃO 02 - GCS -------------- ##########
    # df_ocorrencias['new_date'] = pd.to_datetime(df_ocorrencias['date'])
    # df_ocorrencias['new_date'] = df_ocorrencias['new_date'].dt.tz_convert('America/Sao_Paulo')

    # df_ocorrencias.groupby(['new_date'])

    # log('Writing ocorrencias...')
    # for i, (data_hora, group) in enumerate(df_ocorrencias.groupby(
    # df_ocorrencias['new_date'].dt.date)):
    #     ano_particao = data_hora.strftime('%Y')
    #     mes_particao = data_hora.strftime('%m')
    # file_dir = Path.cwd() / 'pipelines' /
    #     'fogo_cruzado' / 'data' / 'partition_directory' /
    #     f'ano_particao={ano_particao}' / f'mes_particao={mes_particao}'

    #     # Ensure the final directory exists
    #     file_dir.mkdir(parents=True, exist_ok=True)
    #     file_name = f'ocorrencias_{data_hora}.json'
    #     group.to_json(file_dir / file_name, orient='records', lines=True)

    #     if i % 100 == 0:
    #         log(f'iteration: {i}: Files saved to: {file_dir}')
    #     # break


@task(max_retries=5, retry_delay=timedelta(seconds=30))
def load_to_table(project_id: str, dataset_id: str, table_id: str, ocorrencias: list[dict]):
    """
    Save a list of dictionaries to a BigQuery table.

    Args:
        project_id (str): The ID of the GCP project.
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        ocorrencias (list[dict]): The list of dictionaries to be saved to BigQuery.
    """
    log(f"Writing ocorrencias to {project_id}.{dataset_id}.{table_id}")
    save_data_in_bq(
        project_id=project_id, dataset_id=dataset_id, table_id=table_id, json_data=ocorrencias
    )

    # ########## -------------- OPÇÃO 02 - GCS -------------- ##########


# Check if there are any reports returned
@task
def check_report_qty(task_response: list):
    """
    Check if there are any data returned from the API.

    If there are no data returned, log a message and raise a Skipped state
    to stop the flow.
    """
    if not task_response:
        log("No data returned by the API, finishing the flow.", level="info")
        skip = Skipped(message="No data returned by the API, finishing the flow.")
        raise ENDRUN(state=skip)
