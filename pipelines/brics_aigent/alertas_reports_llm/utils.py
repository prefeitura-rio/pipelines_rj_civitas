# -*- coding: utf-8 -*-
"""
This module contains utils for alertas and reports LLM processing.
"""

import re
from datetime import datetime
from typing import Tuple

import basedosdados as bd
import farmhash
import googlemaps
import numpy as np
import pandas as pd
import pytz
from geopy.distance import geodesic
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from prefeitura_rio.pipelines_utils.logging import log
from shapely import wkt

from pipelines.brics_aigent.alertas_reports_llm.classifiers.base import BaseClassifier
from pipelines.utils.environment_vars import getenv_or_action

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True
# from shapely.geometry import Point


tz = pytz.timezone("America/Sao_Paulo")


def fix_bad_formatting(text):
    # remove double spaces
    text = re.sub(r"\s+", " ", text)
    # remove leading and trailing spaces
    text = text.strip()
    return text


def query_data_from_sql_file(path: str, project_id: str, **params) -> pd.DataFrame:
    """
    Run a SQL query from a file, substituting parameters.
    """
    with open(path, "r", encoding="utf-8") as f:
        query = f.read().format(**params)

    client = bigquery.Client(project=project_id)
    return client.query(query).to_dataframe()


def format_address(row):
    """Format address for geocoding."""
    logradouro = str(row.get("logradouro", "")).strip()
    numero = str(row.get("numero", "")).strip()
    bairro = str(row.get("bairro", "")).strip()

    # Build address
    parts = [logradouro]
    if numero and numero not in ["0", "nan", "None"]:
        parts.append(numero)
    if bairro and bairro not in ["nan", "None"]:
        parts.append(bairro)

    # Add Rio de Janeiro to help geocoding
    address = ", ".join(parts) + ", Rio de Janeiro, RJ, Brasil"
    return address


def geocode_address(address: str) -> Tuple[float, float]:
    """
    Geocode an address using Google Maps API.

    Args:
        address: Address string to geocode

    Returns:
        Tuple of (latitude, longitude), returns (0, 0) if geocoding fails
    """
    try:
        # Get API key
        api_key = getenv_or_action("GOOGLE_API_KEY", action="raise")

        # Initialize client
        gmaps = googlemaps.Client(key=api_key)

        # Geocode
        geocode_result = gmaps.geocode(address)

        if geocode_result:
            location = geocode_result[0]["geometry"]["location"]
            return location["lat"], location["lng"]
        else:
            log(f"No geocoding results for: {address}", level="warning")
            return 0, 0

    except Exception as e:
        log(f"Geocoding error for '{address}': {str(e)}", level="error")
        return 0, 0


def load_data_from_dataframe(
    dataframe: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    project_id: str = "rj-civitas",
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> None:
    """
    Load a pandas DataFrame to a BigQuery table.

    Args:
        dataframe: DataFrame to load
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        project_id: GCP project ID
        write_disposition: Write disposition (WRITE_APPEND, WRITE_TRUNCATE)
    """
    try:
        client = bigquery.Client(project=project_id)

        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

        table_ref = client.dataset(dataset_id).table(table_id)

        load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)

        load_job.result()  # Wait for the job to complete

        log(f"Successfully loaded {len(dataframe)} rows to {project_id}.{dataset_id}.{table_id}")

    except Exception as e:
        log(f"Error loading data to BigQuery: {str(e)}", level="error")
        raise


def associar_contextos_proximos(
    df_eventos: pd.DataFrame, df_contextos: pd.DataFrame, raio_buffer: int = 3000
) -> pd.DataFrame:
    """
    Associa eventos a contextos com base em distância geográfica.
    Ignora contextos com geometria inválida.

    Args:
        df_eventos: DataFrame com eventos (deve ter colunas latitude, longitude)
        df_contextos: DataFrame com contextos (deve ter coluna geometria)
        raio_buffer: Buffer adicional em metros para associação

    Returns:
        DataFrame enriquecido com contextos próximos
    """

    def carregar_geom(wkt_str):
        """Carrega geometria WKT, retorna None se inválida."""
        try:
            return wkt.loads(wkt_str) if wkt_str else None
        except Exception as e:
            log(f"Error loading geometry: {str(e)}", level="error")
            return None

    # Preparar contextos
    df_contextos = df_contextos.copy()
    df_contextos["_geom"] = df_contextos["geometria"].apply(carregar_geom)
    df_contextos = df_contextos[
        (df_contextos["_geom"].notnull()) | (df_contextos["cidade_inteira"].fillna(False))
    ]  # remove os inválidos

    log(f"Processing {len(df_eventos)} events against {len(df_contextos)} valid contexts")

    contextos_processados = []

    for _, evento in df_eventos.iterrows():
        lat, lon = evento["latitude"], evento["longitude"]

        # Skip events without coordinates
        if lat == 0 or lon == 0:
            contextos_processados.append({"contextos_proximos": [], "distancia_contexto": []})
            log(
                f"Event {evento['id_report']} has no coordinates. Skipping context association.",
                level="warning",
            )
            continue

        # ponto = Point(lon, lat)
        contextos_match = []
        distancias = []

        for contexto_id, contexto in df_contextos.iterrows():
            if contexto["_geom"] is None:
                log(
                    f"Context {contexto_id} has no geometry. Skipping context association.",
                    level="warning",
                )
                continue

            centro = contexto["_geom"].centroid
            raio = contexto["raio_de_busca"]
            distancia_metros = geodesic((lat, lon), (centro.y, centro.x)).meters

            log(
                f"Context {contexto_id} has distance {distancia_metros} meters from event {evento['id_report']}. Limit is {raio + raio_buffer} meters",
                level="info",
            )

            if distancia_metros <= raio + raio_buffer:
                log(
                    f"Context {contexto_id} is within the buffer. Adding to match list.",
                    level="info",
                )
                contextos_match.append(contexto_id)
                distancias.append(round(distancia_metros))

        contextos_processados.append(
            {"contextos_proximos": contextos_match, "distancia_contexto": distancias}
        )

    # Enriquecer DataFrame original
    df_enriquecido = df_eventos.copy()
    df_enriquecido["contextos_proximos"] = [x["contextos_proximos"] for x in contextos_processados]
    df_enriquecido["distancia_contexto"] = [x["distancia_contexto"] for x in contextos_processados]

    total_associations = sum(len(x["contextos_proximos"]) for x in contextos_processados)
    log(f"Created {total_associations} event-context associations")

    return df_enriquecido


def montar_prompt_relevancia(row: pd.Series, contexto: pd.Series, prompt_template: str) -> str:
    """
    Monta prompt para análise de relevância contextual.

    Args:
        row: Série com dados do evento
        contexto: Série com dados do contexto
        prompt_template: Template do prompt com placeholders

    Returns:
        Prompt formatado para análise
    """
    # Preparar dados do evento
    event_types = (
        ", ".join(row.get("event_types", [])) if isinstance(row.get("event_types"), list) else ""
    )
    locations = (
        ", ".join(row.get("locations", [])) if isinstance(row.get("locations"), list) else ""
    )
    times = ", ".join(row.get("event_time", [])) if isinstance(row.get("event_time"), list) else ""
    people = ", ".join(row.get("people", [])) if isinstance(row.get("people"), list) else ""

    # Preparar dados do contexto
    locais_importantes = (
        ", ".join(contexto.get("locais_importantes", []))
        if isinstance(contexto.get("locais_importantes"), list)
        else ""
    )

    # Formatar prompt usando replace ao invés de format
    return (
        prompt_template.replace("__data_report__", str(row.get("data_report", "")))
        .replace("__categoria__", str(row.get("categoria", "")))
        .replace("__tipo_subtipo__", str(row.get("tipo_subtipo", "")))
        .replace("__orgaos__", str(row.get("orgaos", "")))
        .replace("__descricao__", str(row.get("descricao", "")))
        .replace("__event_types__", event_types)
        .replace("__locations__", locations)
        .replace("__times__", times)
        .replace("__people__", people)
        .replace("__contexto_nome__", str(contexto.get("nome", "")))
        .replace("__contexto_descricao__", str(contexto.get("descricao", "")))
        .replace("__contexto_local__", str(contexto.get("local", "")))
        .replace("__contexto_endereco__", str(contexto.get("endereco", "")))
        .replace("__contexto_raio__", str(contexto.get("raio_de_busca", "")))
        .replace("__locais_importantes__", locais_importantes)
    )


def gerar_prompts_relevancia(
    df_eventos: pd.DataFrame, df_contextos: pd.DataFrame, prompt_template: str
) -> pd.DataFrame:
    """
    Gera prompts para análise de relevância contextual.

    Args:
        df_eventos: DataFrame com eventos enriquecidos (com contextos_proximos)
        df_contextos: DataFrame com contextos indexado por ID
        prompt_template: Template do prompt

    Returns:
        DataFrame com prompts para análise
    """
    prompts = []
    if df_eventos.empty:
        log(
            "No events to analyze for context relevance. Skipping context relevance analysis.",
            level="warning",
        )
        return pd.DataFrame()

    # Filtrar contextos cidade inteira
    contextos_cidade_inteira = df_contextos[df_contextos["cidade_inteira"].fillna(False)]

    # Parte 1: Prompts normais (para eventos com contextos geográficos)
    for idx, row in df_eventos.iterrows():
        for contexto_id in row.get("contextos_proximos", []):
            if contexto_id in df_contextos.index:
                contexto = df_contextos.loc[contexto_id]
                prompts.append(
                    {
                        "id_report": row["id_report"],
                        "contexto_id": contexto_id,
                        "prompt_llm": montar_prompt_relevancia(row, contexto, prompt_template),
                    }
                )

    if prompts:
        log(f"Prompt relevância - contextos geográficos: {prompts[0]}")  # TODO: remove

    # Parte 2: Prompts adicionais para contextos cidade_inteira
    for contexto_id in contextos_cidade_inteira.index:
        for idx, row in df_eventos.iterrows():
            contexto = df_contextos.loc[contexto_id]
            prompts.append(
                {
                    "id_report": row["id_report"],
                    "contexto_id": contexto_id,
                    "prompt_llm": montar_prompt_relevancia(row, contexto, prompt_template),
                }
            )

    if prompts:
        log(f"Prompt relevância - contextos cidade_inteira: {prompts[0]}")  # TODO: remove

    df_prompts = pd.DataFrame(prompts)

    if df_prompts.empty:
        log(
            "No prompts generated for relevance analysis. No event-context pairs found.",
            level="warning",
        )
        return pd.DataFrame()

    # Remove duplicatas (evento pode estar associado a contexto cidade_inteira e geográfico)
    df_prompts = df_prompts.drop_duplicates(subset=["id_report", "contexto_id"])

    log(f"Generated {len(df_prompts)} prompts for relevance analysis")
    log(
        f"Covering {df_prompts['id_report'].nunique()} unique events and {df_prompts['contexto_id'].nunique()} unique contexts"
    )

    return df_prompts


def get_delay_time_string(date_event: pd.Timestamp, tz: pytz.timezone):
    """
    Returns a string with the time difference between the current datetime and the datetime
    in the 'data_ocorrencia' column of the given dataframe.

    Args:
        df_ocorrencias (pd.DataFrame): The dataframe with the 'data_ocorrencia' column.

    Returns:
        str: A string with the time difference (e.g. "3 dias, 2 horas, 1 minuto e 2 segundos").
    """
    delta = datetime.now() - date_event
    delta = datetime.now(tz=tz) - date_event.tz_localize(tz)

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


def hash_string(string):
    return np.uint64(farmhash.fingerprint64(string)).astype("int64")


def assign_id_and_dspy_signature(
    dataframe: pd.DataFrame, classifier: BaseClassifier, id_column: str = "id_report"
) -> pd.DataFrame:
    """Assign unique ID and DSPy signature to each row in the dataframe.

    Args:
        dataframe: DataFrame to process
        classifier: Classifier instance to extract signature from
        id_column: Column name for the ID

    Returns:
        DataFrame with added ID and DSPy signature columns
    """
    signature_desc = classifier.get_input_and_output_descriptions()
    id_values = dataframe[id_column].astype(str).values
    combined = np.char.add(str(signature_desc), id_values)

    dataframe["id"] = [hash_string(s) for s in combined]
    dataframe["dspy_signature"] = [signature_desc] * len(dataframe)

    return dataframe


def get_new_events_ids(
    dataframe: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    id_column: str = "id",
) -> pd.DataFrame:
    """
    Get new events from a table in BigQuery.

    Args:
        dataframe: pd.DataFrame
        project_id: str
        dataset_id: str
        table_id: str
        id_column: str

    Returns:
        list: List of new events ids
    """

    query = f"""
    SELECT DISTINCT {id_column} FROM `{project_id}.{dataset_id}.{table_id}`
    """
    df_new_events = bd.read_sql(query)

    df_new_events = dataframe[~dataframe[id_column].isin(df_new_events[id_column])]
    return df_new_events[id_column].unique()


def check_if_table_exists(project_id: str, dataset_id: str, table_id: str) -> bool:
    """
    Check if a table exists in BigQuery.

    Args:
        project_id: str
        dataset_id: str
        table_id: str

    Returns:
        bool: True if the table exists, False otherwise
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False
