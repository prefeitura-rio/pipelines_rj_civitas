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
from typing import Any, Dict, List, Literal, Optional

import requests
import urllib3
from google.cloud import bigquery
from infisical import InfisicalClient
from prefect import task
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Skipped
from prefeitura_rio.pipelines_utils.infisical import get_secret_folder
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


def get_occurrences(
    token: str,
    initial_date: Optional[str] = None,
    take: int = 1000,
    id_state: str = "b112ffbe-17b3-4ad0-8f2a-2038745d1d14",
) -> List[Dict]:

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
    List
        A list of dictionaries containing the occurrence data.
    """
    occurrences = []

    # Associate parameters names with values
    params_dict = {"initialdate": initial_date, "idState": id_state, "take": take}

    # Filter and create final parameters dictionary with values not None
    params = {k: v for k, v in params_dict.items() if v is not None}

    headers = {"Authorization": f"Bearer {token}"}
    base_url = "https://api-service.fogocruzado.org.br/api/v2/occurrences?page={page}"

    # First request to get the total page number
    initial_url = base_url.format(page=1)
    log(msg="Loop 0: Getting data from API.", level="info")
    response = requests.get(initial_url, headers=headers, params=params)
    response.raise_for_status()
    initial_data = response.json()
    total_pages = initial_data["pageMeta"]["pageCount"]
    occurrences.extend(initial_data["data"])

    # Request next pages
    for page in range(2, total_pages + 1):
        url = base_url.format(page=page)
        log_mod(msg=f"Loop {page}: Getting data from API.", level="info", index=page, mod=10)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        occurrences.extend(data["data"])

    log(msg="Data collected from API successfully.", level="info")

    return occurrences


@task(max_retries=5, retry_delay=timedelta(seconds=30))
def fetch_occurrences(email: str, password: str, initial_date: Optional[str] = None) -> List[Dict]:
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
    List
        A list of dictionaries containing the occurrence data.
    """

    token = auth(email=email, password=password)
    log(msg="Fetching data...", level="info")
    occurrences = get_occurrences(token=token, initial_date=initial_date)
    log(msg="Data fetched successfully.", level="info")

    return occurrences


@task(max_retries=5, retry_delay=timedelta(seconds=30))
def load_to_table(
    project_id: str, dataset_id: str, table_id: str, occurrences: List[Dict[str, Any]]
):
    """
    Save a list of dictionaries to a BigQuery table.

    Args:
        project_id (str): The ID of the GCP project.
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        occurrences (List[Dict]): The list of dictionaries to be saved to BigQuery.
    """
    log(f"Writing occurrences to {project_id}.{dataset_id}.{table_id}")
    SCHEMA = [
        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="documentNumber", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="address", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="state",
            field_type="STRUCT",
            mode="NULLABLE",
            fields=(
                bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
            ),
        ),
        bigquery.SchemaField(
            name="region",
            field_type="STRUCT",
            mode="NULLABLE",
            fields=(
                bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="region", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="state", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="enabled", field_type="STRING", mode="NULLABLE"),
            ),
        ),
        bigquery.SchemaField(
            name="city",
            field_type="STRUCT",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            name="neighborhood",
            field_type="STRUCT",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            name="subNeighborhood",
            field_type="STRUCT",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            name="locality",
            field_type="STRUCT",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(name="latitude", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="longitude", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="date", field_type="TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField(name="policeAction", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="agentPresence", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="relatedRecord", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            name="contextInfo",
            field_type="STRUCT",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField(
                    name="mainReason",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="complementaryReasons",
                    field_type="STRUCT",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="clippings",
                    field_type="STRUCT",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(name="massacre", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="policeUnit", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            name="transports",
            field_type="STRUCT",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="occurrenceId", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    name="transport",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="interruptedTransport", field_type="STRING", mode="NULLABLE"
                ),
                bigquery.SchemaField(name="dateInterruption", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="releaseDate", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    name="transportDescription", field_type="STRING", mode="NULLABLE"
                ),
            ],
        ),
        bigquery.SchemaField(
            name="victims",
            field_type="STRUCT",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="occurrenceId", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="situation", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    name="circumstances",
                    field_type="STRUCT",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(name="deathDate", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="personType", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="age", field_type="INTEGER", mode="NULLABLE"),
                bigquery.SchemaField(
                    name="ageGroup",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="genre",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(name="race", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    name="place",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="serviceStatus",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="qualifications",
                    field_type="STRUCT",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="politicalPosition",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="politicalStatus",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="partie",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="coorporation",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="agentPosition",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    name="agentStatus",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(name="unit", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            name="animalVictims",
            field_type="STRUCT",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="occurrenceId", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    name="animalType",
                    field_type="STRUCT",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(name="situation", field_type="STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    name="circumstances",
                    field_type="STRUCT",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                        bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(name="deathDate", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            name="timestamp_insercao",
            field_type="DATETIME",
            mode="NULLABLE",
            description="Data e hora de inserção no BD em GTM-3",
        ),
    ]

    save_data_in_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema=SCHEMA,
        json_data=occurrences,
    )
    log(f"{len(occurrences)} occurrences written to {project_id}.{dataset_id}.{table_id}")


# Check if there are any reports returned
@task
def check_report_qty(task_response: List):
    """
    Check if there are any data returned from the API.

    If there are no data returned, log a message and raise a Skipped state
    to stop the flow.
    """
    if not task_response:
        log("No data returned by the API, finishing the flow.", level="info")
        skip = Skipped(message="No data returned by the API, finishing the flow.")
        raise ENDRUN(state=skip)


@task
def task_get_secret_folder(
    secret_path: str = "/",
    secret_name: str = None,
    type: Literal["shared", "personal"] = "personal",
    environment: str = None,
    client: InfisicalClient = None,
) -> Dict:
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
