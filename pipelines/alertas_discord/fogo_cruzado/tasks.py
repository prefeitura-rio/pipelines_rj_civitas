# -*- coding: utf-8 -*-
"""
This module contains tasks for sending Fogo Cruzado ocurrences alerts.
"""
import asyncio
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
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.alertas_discord.fogo_cruzado.config import Config
from pipelines.utils import generate_png_map, send_discord_message

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True
tz = pytz.timezone("America/Sao_Paulo")


def get_nearby_cameras(occurrences: pd.DataFrame):
    """
    Get the 5 nearest cameras for each occurrence from the Fogo Cruzado table.

    Parameters
    ----------
    occurrences : pd.DataFrame
        A DataFrame containing the occurrences from the Fogo Cruzado table.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the 5 nearest cameras for each occurrence.
    """
    if occurrences.empty:
        return pd.DataFrame()

    occurrences_ids = "','".join(occurrences["id_ocorrencia"].astype(str).tolist())

    query_nearby_cameras = f"""
        WITH occurrences AS (
            SELECT
                id_ocorrencia,
                latitude,
                longitude
            FROM
                `rj-civitas.fogo_cruzado.ocorrencias`
            WHERE
                id_ocorrencia IN ('{occurrences_ids}')
        ),
        distances AS (
            SELECT
                b.id_ocorrencia,
                a.latitude,
                a.longitude,
                a.id_camera,
                a.nome,
                ST_DISTANCE(
                    ST_GEOGPOINT(a.longitude, a.latitude),
                    ST_GEOGPOINT(b.longitude, b.latitude)
                ) AS distance_meters
            FROM
                `rj-cetrio.ocr_radar.cameras` a
            CROSS JOIN
                occurrences b
        )
        SELECT
            d.*,
            ROW_NUMBER() OVER(PARTITION BY id_ocorrencia ORDER BY distance_meters) AS rn
        FROM
            distances d
        QUALIFY(rn) <= 5
        ORDER BY
            id_ocorrencia,
            distance_meters
    """

    df_nearby_cameras: pd.DataFrame = bd.read_sql(query_nearby_cameras)

    return df_nearby_cameras


@task
def task_get_newest_occurrences(config: Config) -> pd.DataFrame:
    """
    Gets new occurrences from BigQuery based on the given start datetime and reasons.

    Args:
        config (Config): The configuration object containing the start datetime and reasons.

    Returns:
        pd.DataFrame: A DataFrame containing the new occurrences.

    Raises:
        ValueError: If config.reasons is not a list.
    """
    if config.reasons and not isinstance(config.reasons, List):
        print("reasons must be a list")
        raise ValueError("reasons must be a list")

    log("Querying new occurrences from BigQuery...")

    query = f"""
        SELECT
            o.*
        FROM
            `rj-civitas.fogo_cruzado.ocorrencias` o
        LEFT JOIN
            UNNEST(o.motivos_complementares) AS mc
        WHERE
            o.timestamp_insercao > '{config.start_datetime}'
    """

    if config.reasons:
        reasons_str = ("', '".join(config.reasons)).lower()
        query += f"""
            AND (
                LOWER(mc) IN ('{reasons_str}')
                OR LOWER(o.motivo_principal) IN ('{reasons_str}')
            )
        """

    newest_occurrences = bd.read_sql(query)

    if not newest_occurrences.empty:
        log(f"{len(newest_occurrences)} new occurrences found")
        config.newest_occurrences = newest_occurrences

        nearby_cameras = get_nearby_cameras(newest_occurrences)

        for _, occurrence in newest_occurrences.iterrows():
            occurrence_id = occurrence["id_ocorrencia"]
            occurrence_nearby_cameras = nearby_cameras.loc[
                nearby_cameras["id_ocorrencia"] == occurrence_id
            ]

            # Adding nearby cameras
            config.message_manager.add_message(
                occurrence_id=occurrence_id,
                nearby_cameras=occurrence_nearby_cameras,
                timestamp_message=occurrence["data_ocorrencia"],
            )

    return newest_occurrences


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def task_send_discord_messages(config: Config):
    """
    Send messages to discord using the discord webhooks.

    Args:
        config (Config): The configuration object containing the messages contents and webhook url.

    Returns:
        None
    """

    async def main():
        log("Start sending messages to discord.")
        for _, message in config.message_manager.get_all_messages().items():
            if message.get("timestamp_message").date() == datetime.now().date():
                url = config.webhook_url["TIROTEIOS_WEBHOOK_URL"]
            else:
                url = config.webhook_url["TIROTEIOS_RETROATIVO_WEBHOOK_URL"]

            await send_discord_message(
                webhook_url=url,
                message=message.get("content"),
                image_data=message.get("bytes_map", None),
            )

        log("Messages sent to discord successfully.")

    asyncio.run(main())


def get_details(details: list, type: Literal["victim", "animal"] = "victim"):
    """Returns a string with details about victims or animals.

    Args:
        details (list): A list of dictionaries with details about victims or animals.
        type (str, optional): The type of details. Defaults to 'victim'.

    Returns:
        str: A string with details about victims or animals.
    """
    if not isinstance(details, list) or not details:
        return None

    result = []
    for item in details:
        parts = []

        if type == "victim":
            if item["genero_vitima"]:
                parts.append(item["genero_vitima"])
            if item["idade_vitima"]:
                parts.append(f"{item['idade_vitima']} anos")
            if item["situacao_vitima"]:
                parts.append(f"**{item['situacao_vitima'].upper()}**")

        elif type == "animal":
            if item["nome_animal"]:
                parts.append(item["nome_animal"])
            if item["tipo_animal"]:
                parts.append(item["tipo_animal"])
            if item["situacao_animal"]:
                parts.append(f"**{item['situacao_animal'].upper()}**")

        if parts:
            result.append("  - " + ", ".join(parts))

    return "\n".join(result) if result else None


def get_delay_time_string(df_ocorrencias: pd.DataFrame):
    """
    Returns a string with the time difference between the current datetime and the datetime
    in the 'data_ocorrencia' column of the given dataframe.

    Args:
        df_ocorrencias (pd.DataFrame): The dataframe with the 'data_ocorrencia' column.

    Returns:
        str: A string with the time difference (e.g. "3 dias, 2 horas, 1 minuto e 2 segundos").
    """
    delta = datetime.now() - df_ocorrencias["data_ocorrencia"]
    delta = datetime.now(tz=tz) - df_ocorrencias["data_ocorrencia"].tz_localize(tz)

    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Creating a list to store parts of the time string
    time_parts = []

    # Function to add time parts to the list
    def add_time_part(value, singular, plural):
        if value == 1:
            time_parts.append(f"{value} {singular}")
        elif value > 1:
            time_parts.append(f"{value} {plural}")

    # Adding parts for days, hours, minutes, and seconds
    add_time_part(days, "dia", "dias")
    add_time_part(hours, "hora", "horas")
    add_time_part(minutes, "minuto", "minutos")
    add_time_part(seconds, "segundo", "segundos")

    # Joining parts with commas and "and" for the last element
    if time_parts:
        if len(time_parts) == 1:
            time_string = time_parts[0]  # Only one element
        else:
            time_string = ", ".join(time_parts[:-1]) + " e " + time_parts[-1]
    else:
        time_string = "0 segundos"

    return time_string


@task
def task_generate_message(config: Config) -> List[str]:
    """
    Generates a list of messages based on the newest occurrences.

    Args:
        config (Config): Object with the newest occurrences and other configuration parameters.

    Returns:
        List[str]: A list of messages, each one containing information about a single occurrence.

    """

    messages = []
    log("Structuring messages...")
    for _, occurrence in config.newest_occurrences.iterrows():
        list_complementary_reasons = [f"  - {i}" for i in occurrence["motivos_complementares"]]
        complementary_reasons = "\n".join(list_complementary_reasons)

        list_victims = [i for i in occurrence["vitimas"]]
        list_animal_victims = [i for i in occurrence["vitimas_animais"]]

        victims_details = get_details(list_victims, type="victim")
        animal_victims_details = get_details(list_animal_victims, type="animal")

        timestamp_message = occurrence["data_ocorrencia"]
        # Building message
        message = (
            f"**TIROTEIO REPORTADO**\n\n"
            f"- **Atraso**: {get_delay_time_string(occurrence)}\n"
            f"- **Data**: {timestamp_message.strftime('%Y-%m-%d')}\n"
            f"- **Horário**: {occurrence['data_ocorrencia'].strftime('%H:%M:%S')}\n"
            f"- **Local**: {occurrence['endereco']}\n"
            f"- **Latitute e Longitude**: {occurrence['latitude']} {occurrence['longitude']}\n\n"
            f"- **Motivo Principal**:\n"
            f"  - {occurrence['motivo_principal']}\n\n"
        )

        # Adding reasons
        if complementary_reasons:
            message += f"- **Motivos Secundários**:\n{complementary_reasons}\n\n"

        police_action = (
            ":ballot_box_with_check:" if occurrence["acao_policial"] == "true" else ":x:"
        )
        agent_presence = (
            ":ballot_box_with_check:"
            if occurrence["presenca_agente_seguranca"] == "true"
            else ":x:"
        )
        massacre = ":ballot_box_with_check:" if occurrence["massacre"] == "true" else ":x:"

        message += (
            f"- **Ação Policial**: {police_action}\n"
            f"- **Presença de Agentes de Segurança**: {agent_presence}\n"
            f"- **Massacre**: {massacre}\n\n"
        )

        # Adding victims
        if victims_details:
            message += f"- **Vítimas**:\n{victims_details}\n\n"

        # Adding as animal victims
        if animal_victims_details:
            message += f"- **Vítimas Animais**:\n{animal_victims_details}\n\n"

        # Adding nearby cameras
        nearby_cameras = config.message_manager.get_message(occurrence["id_ocorrencia"]).get(
            "nearby_cameras", pd.DataFrame()
        )

        cameras_strings = []
        if not nearby_cameras.empty:
            cameras_strings.append("- **Câmeras mais próximas**:")
            for j, camera in nearby_cameras.iterrows():
                cameras_strings.append(
                    f"  - {j + 1} - {camera['id_camera']} "
                    + f"({camera['nome'].upper()}) - {camera['distance_meters']:.2f}m"
                )

        message += "\n".join(cameras_strings)

        messages.append(message)

        params = [
            {
                "key": "content",
                "value": message,
            },
            {
                "key": "timestamp_message",
                "value": timestamp_message,
            },
        ]

        config.message_manager.update_multiple_messages(
            occurrence_id=occurrence["id_ocorrencia"], updates=params
        )

    log(f"Generated {len(messages)} messages.")
    return messages


@task
def task_generate_png_maps(config: Config, zoom_start: int = 10):
    """
    Generates a list of PNG maps as Bytes based on the newest occurrences.

    Args:
        config (Config): Object with the newest occurrences and other configuration parameters.
        zoom_start (int, optional): The initial zoom level for the map. Defaults to 10.

    Returns:
        List: A list of PNG maps as Bytes.
    """

    maps = []
    log("Generating PNG maps...")

    for _, occurrence in config.newest_occurrences.iterrows():
        latitude = occurrence["latitude"]
        longitude = occurrence["longitude"]
        id = occurrence["id_ocorrencia"]

        png_map = generate_png_map(
            locations=[(latitude, longitude)],
            zoom_start=zoom_start,
            nearby_cameras=config.message_manager.get_message(id).get(
                "nearby_cameras", pd.DataFrame()
            ),
        )

        maps.append(png_map)

        config.message_manager.update_message(
            occurrence_id=occurrence["id_ocorrencia"], key="bytes_map", value=png_map
        )

    return maps


@task
def task_check_occurrences_qty(config: Config):
    if config.newest_occurrences.empty:
        log("No data returned by the API, finishing the flow.")
        skip = Skipped(message="No data returned by the API, finishing the flow.")
        raise ENDRUN(state=skip)


@task
def task_set_config(**kwargs):
    """
    Set configuration parameters for the execution.
    """
    config = Config(**kwargs)
    return config


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
