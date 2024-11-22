# -*- coding: utf-8 -*-
"""
This module contains tasks for retrieving, processing, and saving XML reports.

Tasks include:
- Retrieving reports from a REST API
- Saving reports as XML files
- Parsing and normalizing XML data
- Transforming XML data into structured CSV files
"""


import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal

import basedosdados as bd
import googlemaps
import pandas as pd
import pytz
import urllib3
from google.cloud import bigquery
from infisical import InfisicalClient
from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_secret_folder
from prefeitura_rio.pipelines_utils.logging import log, log_mod
from pytz import timezone

from pipelines.scraping_redes.models.model import EnrichResponseModel, Model
from pipelines.scraping_redes.models.palver import Palver
from pipelines.scraping_redes.utils.utils import (
    check_if_table_exists,
    get_default_value_for_field,
    get_state_from_components,
    load_data_from_dataframe,
    save_data_in_bq,
    skip_flow_run,
)

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True


tz = timezone("America/Sao_Paulo")
# Disable the warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
    log(
        f"Getting secrets from Infisical: secret_path={secret_path}, secret_name={secret_name}, type={type}, environment={environment}"
    )
    secrets = get_secret_folder(
        secret_path=secret_path,
        secret_name=secret_name,
        type=type,
        environment=environment,
        client=client,
    )
    return secrets


@task
def task_get_channels_names_from_bq(project_id: str, dataset_id: str, table_id: str):
    log(
        f"Getting channels names from BigQuery: project_id={project_id}, dataset_id={dataset_id}, table_id={table_id}"
    )
    query = rf"""
        SELECT DISTINCT
            TRIM(
                REGEXP_EXTRACT(
                    REGEXP_REPLACE(twitter, r'^@', ''),
                    r'[^/]+$'
                )
            ) AS chat_username
        FROM
            `{project_id}.{dataset_id}.{table_id}`
        WHERE
            twitter IS NOT NULL
    """
    df = bd.read_sql(query)
    return df["chat_username"].tolist()


@task
def task_set_palver_variables(base_url: str, token: str):
    log("Setting Palver variables..")
    Palver.set_base_url(base_url)
    Palver.set_token(token)


@task
def task_get_chats(
    destination_path: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    chat_usernames: List[str],
    mode: Literal["prod", "staging"] = "staging",
) -> List[Dict[str, Any]]:
    log(f"Getting chats IDs from Palver for chat usernames: {chat_usernames}")
    chats = []
    dataset_id += "_staging" if mode == "staging" else ""
    table_exists = check_if_table_exists(
        dataset_id="scraping_redes", table_id="twitter_chats", mode=mode
    )
    destination_path = Path(destination_path)
    destination_path.mkdir(parents=True, exist_ok=True)

    if table_exists:
        query = rf"""SELECT
            username
        FROM
            `{project_id}.{dataset_id}.{table_id}`"""
        usernames_in_table = bd.read_sql(query)
        chat_usernames = [
            username
            for username in chat_usernames
            if username not in usernames_in_table["username"].tolist()
        ]

    for i, username in enumerate(chat_usernames):
        chat = Palver.get_chats(
            source_name="twitter", query=f"c_username: ('{username}')", page=1, page_size=1
        )
        if chat:
            log(f"Found chat for username: {username} - chat: {chat}")
            chat[0].update({"username": username})
            chats.extend(chat)

            df_chats = pd.DataFrame(chat)

            df_chats.to_csv(
                f"{destination_path}/{username}.csv", sep=",", quotechar='"', quoting=2, index=False
            )
            log(f"Files saved in {destination_path}/{username}.csv")

        else:
            log(f"No chat found for username: {username}")

    log(f"Found {len(chats)} new chats")
    return chats


# @task
def get_chats_last_dates(
    project_id: str, dataset_id: str, table_id: str, chats_ids: List[str]
) -> Dict[str, str]:

    # chats_ids = [chat["id"] for chat in chats]

    data = bd.read_sql(
        f"""
        SELECT
            chat_id,
            MAX(datetime) as last_date
        FROM
            `{project_id}.{dataset_id}.{table_id}`
        WHERE
            chat_id IN ('{"', '".join(chats_ids)}')
        GROUP BY
            chat_id
    """
    )

    dict_data = dict(zip(data["chat_id"], data["last_date"]))

    return dict_data


@task
def task_get_messages(
    project_id: str,
    dataset_id: str,
    table_id: str,
    start_date: str = None,
    end_date: str = None,
    mode: Literal["prod", "staging"] = "staging",
) -> List[Dict[str, Any]]:

    dataset_id += "_staging" if mode == "staging" else ""

    if end_date is None or end_date == "":
        end_date = datetime.now(tz=pytz.utc) + timedelta(days=1)

    chats = bd.read_sql(f"SELECT id FROM `{project_id}.{dataset_id}.twitter_chats`")

    # chats_ids = [chat["id"] for chat in chats]
    chats_ids = chats["id"].tolist()
    table_exists = check_if_table_exists(dataset_id=dataset_id, table_id=table_id, mode="prod")
    if table_exists:
        last_dates = get_chats_last_dates(
            project_id=project_id, dataset_id=dataset_id, table_id=table_id, chats_ids=chats_ids
        )
    else:
        last_dates = {}

    messages = []
    log(f"Getting messages from Palver for chat IDs: {chats_ids}")
    for chat in chats_ids:
        query_message = f"chat_id: ('{chat}')"

        if last_dates.get(chat, None):
            last_date = last_dates[chat]

            # Convert to datetime if it's a string
            if isinstance(last_date, str):
                try:
                    last_date = datetime.strptime(last_date, "%Y-%m-%dT%H:%M:%S")
                except ValueError as e:
                    raise ValueError(f"Invalid date format for chat {chat}: {e}")

            # Check if it's a datetime object
            if not isinstance(last_date, datetime):
                raise ValueError(f"last_dates[{chat}] is not a datetime object")

            last_date += timedelta(seconds=1)
            start_date = last_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        message = Palver.get_messages(
            source_name="twitter",
            query=query_message,
            start_date=start_date,
            end_date=end_date,
        )

        if message:
            messages.extend(message)

    columns = [
        "id",
        "chat_id",
        "sender_id",
        "datetime",
        "text",
        "sentiment",
        "transcript_lang",
        "ocr_lang",
        "text_lang",
        "lang",
        "is_spam",
        "is_nsfw",
        "transcript",
        "ocr",
        "is_potentially_misleading",
        "is_news_related",
    ]

    # rebuild dict with only selected columns
    selected_messages = [
        {
            key: message[key].strip('"') if isinstance(message[key], str) else message[key]
            for key in columns
            if key in message
        }
        for message in messages
    ]

    if len(selected_messages) == 0:
        message = f"No messages found for chat IDs: {chats_ids}"
        skip_flow_run(message)

    log(f"Found {len(selected_messages)} messages")
    return selected_messages


@task(max_retries=5, retry_delay=timedelta(seconds=30))
def task_load_to_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    occurrences: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
    mode: Literal["prod", "staging"] = "staging",
):
    """
    Save a list of dictionaries to a BigQuery table.

    Args:
        project_id (str): The ID of the GCP project.
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        occurrences (List[Dict]): The list of dictionaries to be saved to BigQuery.
    """
    dataset_id += "_staging" if mode == "staging" else ""

    if len(occurrences) == 0 or occurrences is None:
        log(f"No occurrences to write to {project_id}.{dataset_id}.{table_id}")
        return None

    log(f"write_disposition for table {table_id}: {write_disposition}")
    log(f"Writing occurrences to {project_id}.{dataset_id}.{table_id}")
    if table_id.endswith("messages"):
        SCHEMA = [
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="sender_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="sentiment", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="transcript_lang", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="ocr_lang", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="text_lang", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="lang", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="is_spam", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="is_nsfw", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="transcript", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="ocr", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                name="is_potentially_misleading", field_type="BOOLEAN", mode="NULLABLE"
            ),
            bigquery.SchemaField(name="is_news_related", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(
                name="timestamp_creation", field_type="timestamp", mode="NULLABLE"
            ),
        ]
    elif table_id.endswith("chats"):
        SCHEMA = [
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="source", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="username", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                name="timestamp_creation", field_type="TIMESTAMP", mode="NULLABLE"
            ),
        ]
    else:
        raise ValueError(f"Table {table_id} not supported")

    save_data_in_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema=SCHEMA,
        json_data=occurrences,
        write_disposition=write_disposition,
    )
    log(f"{len(occurrences)} occurrences written to {project_id}.{dataset_id}.{table_id}")


@task
def task_get_date_execution(utc: bool = False) -> str:
    if utc:
        date_execution = datetime.now(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    else:
        date_execution = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")

    return date_execution


@task
def task_get_llm_reponse_and_update_table(
    # dataframe: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    query: str = None,
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
    mode: Literal["prod", "staging"] = "staging",
) -> None:
    dataset_id += "_staging" if mode == "staging" else ""

    table_enriquecimento_exists = check_if_table_exists(
        dataset_id=dataset_id, table_id=table_id, mode="prod"
    )

    if table_enriquecimento_exists:
        query += rf"""
        LEFT JOIN
            {project_id}.{dataset_id}.{table_id} b
        ON
            a.id = b.id
        WHERE
            b.id IS NULL"""

    dataframe = (bd.read_sql(query)).reset_index()

    if len(dataframe) > 0:

        log(f"Start generate llm response for {prompt_column} with {len(dataframe)} rows")
        if prompt_column == "prompt_column":
            response_schema = EnrichResponseModel.schema()
        else:
            raise ValueError("prompt_column must be 'prompt_column'")

        schema = [
            bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="prompt_column", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="is_news_related", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="locality", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="date_execution", field_type="TIMESTAMP", mode="NULLABLE"),
        ]

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

        table_exists = check_if_table_exists(dataset_id=dataset_id, table_id=table_id, mode="prod")

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


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_geocode_localities(
    project_id: str,
    dataset_id: str,
    table_id: str,
    mode: Literal["prod", "staging"] = "staging",
    api_key: str = None,
) -> List[Dict]:
    """Geocodes localities from twitter_enriquecido table using Google Geocoding API.

    Args:
        project_id (str): BigQuery project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID containing enriched data
        mode (Literal["prod", "staging"]): Execution mode. Defaults to "staging".

    Returns:
        List[Dict]: List of dictionaries containing geocoded data
    """
    dataset_id += "_staging" if mode == "staging" else ""

    # Initialize Google Maps client with service account credentials
    client = googlemaps.Client(key=api_key)

    table_twitter_georreferenciado_exists = check_if_table_exists(
        dataset_id=dataset_id, table_id="twitter_georreferenciado", mode="prod"
    )

    # Get data from twitter_enriquecido
    query = f"""
    SELECT DISTINCT
        a.id,
        a.text,
        a.locality,
        a.is_news_related,
        a.date_execution
    FROM `{project_id}.{dataset_id}.{table_id}` a
    """

    if table_twitter_georreferenciado_exists:
        query += f"""
        LEFT JOIN
            {project_id}.{dataset_id}.twitter_georreferenciado b
        ON
            a.id = b.id
        WHERE
            b.id IS NULL
            AND a.locality IS NOT NULL
            AND a.locality != ''"""
    else:
        query += """
        WHERE
            locality IS NOT NULL
            AND locality != ''"""

    # keep only news related messages
    query += """
        AND a.is_news_related = True"""

    log(f"QUERY GEOREF: \n{query}")
    df = bd.read_sql(query)

    if len(df) == 0:
        log("No localities to geocode")
        return []

    geocoded_data = []

    log(f"Geocoding localities from {dataset_id}.{table_id}")
    for i, row in df.iterrows():
        log_mod(f"Geocoding locality {i}/{len(df)}", index=i, mod=100)
        try:
            # Add "Rio de Janeiro" to improve geocoding accuracy
            search_text = row["locality"]

            # Get geocoding results
            geocode_result = client.geocode(
                address=search_text,
                region="br",  # Restrict to Brazil
            )

            if geocode_result:
                result = geocode_result[0]
                location = result["geometry"]["location"]
                state = get_state_from_components(result["address_components"])

                geocoded_data.append(
                    {
                        "id": row["id"],
                        "text": row["text"],
                        "locality": row["locality"],
                        "latitude": location["lat"],
                        "longitude": location["lng"],
                        "formatted_address": result["formatted_address"],
                        "state": state if state else "",
                        "is_news_related": row["is_news_related"],
                    }
                )

        except Exception as e:
            log(f"Error geocoding locality {row['locality']}: {str(e)}")
            continue

    log(f"Successfully geocoded {len(geocoded_data)} localities")
    return geocoded_data


@task
def task_save_geocoded_data(
    project_id: str,
    dataset_id: str,
    table_id: str,
    geocoded_data: List[Dict],
    mode: Literal["prod", "staging"] = "staging",
) -> None:
    """Saves geocoded data to BigQuery table.

    Args:
        project_id (str): BigQuery project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID for geocoded data
        geocoded_data (List[Dict]): List of dictionaries containing geocoded data
        mode (Literal["prod", "staging"]): Execution mode. Defaults to "staging".
    """
    if not geocoded_data:
        log("No geocoded data to save")
        return None

    dataset_id += "_staging" if mode == "staging" else ""

    schema = [
        bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="locality", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="latitude", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="longitude", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="formatted_address", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="state", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="is_news_related", field_type="BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField(name="timestamp_creation", field_type="TIMESTAMP", mode="NULLABLE"),
    ]

    save_data_in_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema=schema,
        json_data=geocoded_data,
        write_disposition="WRITE_APPEND",
    )
