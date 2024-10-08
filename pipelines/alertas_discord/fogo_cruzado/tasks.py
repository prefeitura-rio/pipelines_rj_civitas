# -*- coding: utf-8 -*-
"""
This module contains tasks for sending Fogo Cruzado ocurrences alerts.
"""
import asyncio
from datetime import datetime
from typing import List, Literal

import basedosdados as bd
import pandas as pd
import pytz
from prefect import task
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Skipped
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils import generate_png_map, send_discord_message

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True
tz = pytz.timezone("America/Sao_Paulo")


@task
def get_newest_occurrences(start_datetime: str = None, reasons: List[str] = None) -> pd.DataFrame:
    """
    Get the newest occurrences from BigQuery.

    Parameters
    ----------
    start_datetime : str, optional
        The start datetime to filter the newest occurrences. Defaults to None.
    reasons : List[str], optional
        A list of the reasons to filter the newest occurrences. Defaults to None.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the newest occurrences from BigQuery.
    """
    if not isinstance(reasons, List):
        print("reasons must be a list")
        raise ValueError("reasons must be a list")

    log("Querying new occurrences from BigQuery...")

    query = f"""SELECT
                o.*
            FROM
                `rj-civitas.fogo_cruzado.ocorrencias` o
            LEFT JOIN (
                SELECT
                    o.id_ocorrencia,
                    mc AS motivos_complementares
                FROM
                    `rj-civitas.fogo_cruzado.ocorrencias` o,
                    UNNEST(motivos_complementares) mc
                WHERE
                    lower(mc) IN ('disputa')
            ) mc
            ON o.id_ocorrencia = mc.id_ocorrencia
            WHERE
                timestamp_insercao > '{start_datetime}'"""
    if reasons:
        query += f"""
                AND (
                    lower(o.motivo_principal) IN ({', '.join(f"'{reason}'" for reason in reasons)})
                    OR lower(mc.motivos_complementares) IN ({', '.join(
                        f"'{reason}'" for reason in reasons)})
                );
        """
    newest_occurrences = bd.read_sql(query)

    return newest_occurrences


@task
def task_send_discord_messages(webhook_url: str, messages: list[str], images: list[bytes] = None):
    async def main(url_webhook, messages, images):
        """Send a message to a Discord webhook.

        Args:
            url_webhook (str): The URL of the webhook.
            message (str): The message to send.
            image_data (bytes): The PNG image data to embed.
        """
        log("Start sending messages to discord.")
        for message, image_data in zip(messages, images):
            await send_discord_message(
                webhook_url=url_webhook, message=message, image_data=image_data
            )

        log("Messages sent to discord successfully.")

    asyncio.run(main(webhook_url, messages, images))


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
def generate_message(newest_occurrences: pd.DataFrame):
    """Returns a list of texts with details about the newest occurrences formatted to discord.

    Args:
        newest_occurrences (pd.DataFrame): A DataFrame with the newest occurrences.

    Returns:
        list: A list of strings with details about the newest occurrences.
    """
    messages = []
    log("Structuring messages...")
    for _, occurrence in newest_occurrences.iterrows():
        list_complementary_reasons = [f"  - {i}" for i in occurrence["motivos_complementares"]]
        complementary_reasons = "\n".join(list_complementary_reasons)

        list_victims = [i for i in occurrence["vitimas"]]
        list_animal_victims = [i for i in occurrence["vitimas_animais"]]

        victims_details = get_details(list_victims, type="victim")
        animal_victims_details = get_details(list_animal_victims, type="animal")

        # Building message
        message = (
            f"**TIROTEIO REPORTADO**\n\n"
            f"- **Atraso**: {get_delay_time_string(occurrence)}\n"
            f"- **Data**: {occurrence['data_ocorrencia'].strftime('%Y-%m-%d')}\n"
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

        messages.append(message)

    log(f"Generated {len(messages)} messages.")
    return messages


@task
def task_generate_png_maps(occurrences: pd.DataFrame, zoom_start: int = 10):
    """
    Generate a list of PNG maps using Folium given a list of locations and an initial zoom.

    Args:
        occurrences (pd.DataFrame): A DataFrame with the newest occurrences.
        zoom_start (int, optional): The initial zoom level. Defaults to 10.

    Returns:
        list: A list of PNG image data.
    """
    maps = []
    log("Generating PNG maps...")
    for _, occurrence in occurrences.iterrows():
        latitude = occurrence["latitude"]
        longitude = occurrence["longitude"]

        png_map = generate_png_map([(latitude, longitude)], zoom_start=zoom_start)
        maps.append(png_map)

    return maps


@task
def check_occurrences_qty(occurrences: pd.DataFrame):
    if len(occurrences) == 0:
        log("No data returned by the API, finishing the flow.")
        skip = Skipped(message="No data returned by the API, finishing the flow.")
        raise ENDRUN(state=skip)
