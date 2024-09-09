# -*- coding: utf-8 -*-
import json
from datetime import datetime
from typing import Any, Dict, List

import pytz
from google.cloud import bigquery

try:
    from prefect.engine.state import State
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect", "sentry_sdk"], extras=["pipelines"])

from prefeitura_rio.pipelines_utils.infisical import get_infisical_client, inject_env
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode

tz = pytz.timezone("America/Sao_Paulo")


def save_data_in_bq(
    project_id: str, dataset_id: str, table_id: str, json_data: List[Dict[str, Any]]
) -> None:
    """
    Saves a list of dictionaries to a BigQuery table.

    Args:
        project_id: The ID of the GCP project.
        dataset_id: The ID of the dataset.
        table_id: The ID of the table.
        json_data: The list of dictionaries to be saved to BigQuery.

    Raises:
        Exception: If there is an error while inserting the data into BigQuery.
    """

    client = bigquery.Client()
    table_full_name = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        # schema=schema,
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE",
        # time_partitioning=bigquery.TimePartitioning(
        #     type_=bigquery.TimePartitioningType.DAY,
        #     field="data_particao",  # name of column to use for partitioning
        # ),
    )

    # Adding timestamp inside 'date' dict
    json_data = [
        {
            **data,
            "date": {
                **data["date"],
                "timestamp_insercao": datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S"),
            },
        }
        for data in json_data
    ]

    json_data = json.loads(json.dumps([json_data]))
    try:
        job = client.load_table_from_json(json_data, table_full_name, job_config=job_config)
        job.result()
    except Exception:
        raise Exception(json_data)


def inject_fogocruzado_credentials() -> None:
    """
    Loads FOGOCRUZADO credentials from Infisical into environment variables.
    """
    client = get_infisical_client()

    environment = get_flow_run_mode()

    for secret_name in [
        "FOGOCRUZADO_USERNAME",
        "FOGOCRUZADO_PASSWORD",
    ]:
        inject_env(
            secret_name=secret_name,
            environment=environment,
            client=client,
        )


def handler_inject_fogocruzado_credentials(obj, old_state: State, new_state: State) -> State:
    """
    State handler that will inject FOGOCRUZADO credentials into the environment.
    """
    if new_state.is_running():
        inject_fogocruzado_credentials()
    return new_state
