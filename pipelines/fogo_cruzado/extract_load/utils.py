# -*- coding: utf-8 -*-
# import json
from datetime import datetime
from typing import Any, Dict, List

import pytz
from google.cloud import bigquery

tz = pytz.timezone("America/Sao_Paulo")


def save_data_in_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    schema: List[bigquery.SchemaField],
    json_data: List[Dict[str, Any]],
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
        write_disposition="WRITE_TRUNCATE",
        # time_partitioning=bigquery.TimePartitioning(
        #     type_=bigquery.TimePartitioningType.DAY,
        #     field="data_particao",  # name of column to use for partitioning
        # ),
    )

    # Adding timestamp
    json_data = [
        {
            **data,
            "timestamp_insercao": datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S"),
        }
        for data in json_data
    ]

    try:
        job = client.load_table_from_json(json_data, table_full_name, job_config=job_config)
        job.result()
    except Exception as e:
        raise Exception(e)
