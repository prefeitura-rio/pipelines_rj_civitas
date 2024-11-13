# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Any, Dict, List, Literal

import basedosdados as bd
import pandas as pd
import pytz
from google.cloud import bigquery
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Skipped
from prefeitura_rio.pipelines_utils.logging import log


def check_if_table_exists(dataset_id: str, table_id: str, mode: Literal["prod", "staging"]) -> bool:
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    return tb.table_exists(mode=mode)


def save_data_in_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    schema: List[bigquery.SchemaField],
    json_data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
) -> None:
    """
    Saves a list of dictionaries to a BigQuery table.

    Args:
        project_id: The ID of the GCP project.
        dataset_id: The ID of the dataset.
        table_id: The ID of the table.
        schema: List of BigQuery table schema.
        json_data: The list of dictionaries to be saved to BigQuery.

    Raises:
        Exception: If there is an error while inserting the data into BigQuery.
    """
    client = bigquery.Client()
    table_full_name = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition=write_disposition,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="timestamp_creation",  # name of column to use for partitioning
        ),
        clustering_fields=["timestamp_creation"],
    )
    # Adicionando timestamp de criação em UTC para cada registro
    # for data in json_data:
    #     data["timestamp_creation"] = datetime.now(tz=pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # # Adding timestamp
    # for data in json_data:
    #     data.update({
    #         "timestamp_creation": datetime.now(tz=pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    #     })
    json_data = [
        {
            **data,
            "timestamp_creation": datetime.now(tz=pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        for data in json_data
    ]

    try:
        print(json_data)
        job = client.load_table_from_json(json_data, table_full_name, job_config=job_config)
        job.result()
    except Exception as e:
        raise Exception(e)


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


def skip_flow_run(message: str):
    log(message)
    skip = Skipped(message=message)
    raise ENDRUN(state=skip)
