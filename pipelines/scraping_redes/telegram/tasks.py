# -*- coding: utf-8 -*-
"""
This module contains tasks for retrieving, processing, and saving XML reports.

Tasks include:
- Retrieving reports from a REST API
- Saving reports as XML files
- Parsing and normalizing XML data
- Transforming XML data into structured CSV files
"""


from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal

import basedosdados as bd
import urllib3
from google.cloud import bigquery
from infisical import InfisicalClient
from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_secret_folder
from prefeitura_rio.pipelines_utils.logging import log
from pytz import timezone

from pipelines.scraping_redes.models.palver import Palver
from pipelines.scraping_redes.utils.utils import check_if_table_exists, save_data_in_bq

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
        SELECT
            DISTINCT REGEXP_EXTRACT(telegram, r't\.me/(.*)') AS chat_username
        FROM
            `{project_id}.{dataset_id}.{table_id}`
        WHERE
            telegram IS NOT NULL
    """
    df = bd.read_sql(query)
    return df["chat_username"].tolist()


@task
def task_set_palver_variables(base_url: str, token: str):
    log(f"Setting Palver variables: base_url={base_url}, token={token}")
    Palver.set_base_url(base_url)
    Palver.set_token(token)


@task
def task_get_chats(
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
        dataset_id="scraping_redes", table_id="telegram_chats", mode=mode
    )

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
        chats.extend(
            Palver.get_chats(
                source_name="telegram", query=f"username: ({username})", page=1, page_size=1
            )
        )
        chats[i].update({"username": username})

    # query_chats = "username: (" + " OR ".join(chat_usernames) + ")"
    # chats = Palver.get_chats(source_name="telegram", query=query_chats)
    log(f"Found {len(chats)} chats")
    log(chats)
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


# def task_get_chats_ids(chats: List[Dict[str, Any]]) -> List[str]:
@task
def task_get_messages(
    project_id: str,
    dataset_id: str,
    table_id: str,
    chats: List[Dict[str, Any]],
    start_date: str = None,
    end_date: str = None,
) -> List[Dict[str, Any]]:
    chats_ids = [chat["id"] for chat in chats]

    last_dates = get_chats_last_dates(
        project_id=project_id, dataset_id=dataset_id, table_id=table_id, chats_ids=chats_ids
    )
    messages = []

    for chat in chats_ids:
        query_message = f"chat_id: ({chat})"

        if last_dates.get(chat, None):
            last_date = datetime.strptime(last_dates[chat], "%Y-%m-%dT%H:%M:%SZ") + timedelta(
                seconds=1
            )
            start_date = last_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        log(f"Getting messages from Palver for chat username: {query_message}")
        messages.extend(
            Palver.get_messages(
                source_name="telegram",
                query=query_message,
                start_date=start_date,
                end_date=end_date,
            )
        )
    # query_messages = "chat_id: (" + " OR ".join(chats_ids) + ")"
    # log(f"Getting messages from Palver for chat usernames: {query_messages}")
    # messages = Palver.get_messages(
    # source_name="telegram", query=query_messages, start_date=start_date, end_date=end_date
    # )

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

    return selected_messages


@task(max_retries=5, retry_delay=timedelta(seconds=30))
def task_load_to_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    occurrences: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
):
    """
    Save a list of dictionaries to a BigQuery table.

    Args:
        project_id (str): The ID of the GCP project.
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        occurrences (List[Dict]): The list of dictionaries to be saved to BigQuery.
    """
    if len(occurrences) == 0 or occurrences is None:
        log(f"No occurrences to write to {project_id}.{dataset_id}.{table_id}")
        return None

    log(f"write_disposition for table {table_id}: {write_disposition}")
    log(f"Writing occurrences to {project_id}.{dataset_id}.{table_id}")
    if table_id == "telegram_messages":
        SCHEMA = [
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="sender_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime", field_type="STRING", mode="NULLABLE"),
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
    elif table_id == "telegram_chats":
        SCHEMA = [
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="participants", field_type="INT64", mode="NULLABLE"),
            bigquery.SchemaField(name="source", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="username", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                name="timestamp_creation", field_type="timestamp", mode="NULLABLE"
            ),
        ]
    else:
        raise ValueError(f"Table {table_id} not supported")

    log(SCHEMA)
    save_data_in_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema=SCHEMA,
        json_data=occurrences,
        write_disposition=write_disposition,
    )
    log(f"{len(occurrences)} occurrences written to {project_id}.{dataset_id}.{table_id}")
