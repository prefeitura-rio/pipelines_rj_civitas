# -*- coding: utf-8 -*-
# import json
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
    schema = [
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
        bigquery.SchemaField(name="latitude", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="longitude", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="date", field_type="STRING", mode="NULLABLE"),
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
                bigquery.SchemaField(name="age", field_type="STRING", mode="NULLABLE"),
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
        bigquery.SchemaField(name="timestamp_insercao", field_type="STRING", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
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
            "timestamp_insercao": datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S"),
        }
        for data in json_data
    ]

    # json_data = json.loads(json.dumps(json_data))
    try:
        job = client.load_table_from_json(json_data, table_full_name, job_config=job_config)
        job.result()
    except Exception as e:
        raise Exception(e)


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
