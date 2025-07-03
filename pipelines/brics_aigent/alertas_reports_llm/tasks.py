# -*- coding: utf-8 -*-
"""
This module contains tasks for BRICS alerts and reports processing using LLM.
"""
import asyncio
import os
from datetime import datetime
from typing import List, Optional

import basedosdados as bd
import dspy
import pandas as pd
import pytz
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.brics_aigent.alertas_reports_llm.classifiers import ClassifierFactory
from pipelines.brics_aigent.alertas_reports_llm.utils import (
    associar_contextos_proximos,
    format_address,
    geocode_address,
    gerar_prompts_relevancia,
    get_delay_time_string,
    load_data_from_dataframe,
)
from pipelines.utils.environment_vars import getenv_or_action
from pipelines.utils.notifications import send_discord_message

bd.config.billing_project_id = "rj-civitas"
bd.config.from_file = True
tz = pytz.timezone("America/Sao_Paulo")


@task
def task_configure_dspy(
    api_key: str | None = None,
    model_name: str = "gemini/gemini-2.5-flash",
    temperature: float = 0.5,
    max_tokens: int = 1024,
) -> dict:
    """
    Configure DSPy globally for the flow execution.

    Args:
        api_key: API key for the LLM
        model_name: Name of the LLM model
        temperature: Temperature for generation
        max_tokens: Maximum tokens for response

    Returns:
        Dictionary with configuration details
    """
    if not api_key:
        api_key = getenv_or_action("GOOGLE_API_KEY", action="raise")

    # Create and configure DSPy LM
    lm = dspy.LM(
        model_name,
        api_key=api_key,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    # Configure DSPy globally
    dspy.configure(lm=lm, track_usage=True)

    log(f"DSPy configured globally with {model_name}")

    return {
        "model_name": model_name,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "configured": True,
    }


@task
def task_get_date_execution() -> str:
    """
    Returns the current date and time in the UTC timezone.
    """
    return datetime.now(tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S")


@task
def task_get_events(
    query_template: str,
    minutes_interval: int | None = None,
    start_datetime: str | None = None,
    end_datetime: str | None = None,
) -> pd.DataFrame:
    """
    Get events from BigQuery with time filtering.

    Args:
        query_template: SQL query template with placeholders
        minutes_interval: Minutes interval for recent data
        start_datetime: Start datetime filter
        end_datetime: End datetime filter

    Returns:
        Raw DataFrame with events
    """

    # Input validation
    if not minutes_interval and not (start_datetime and end_datetime):
        error_msg = (
            "Invalid parameters: Either 'minutes_interval' OR both "
            "'start_datetime' and 'end_datetime' must be provided"
        )
        log(error_msg, level="error")
        raise ValueError(error_msg)

    # Query construction
    if minutes_interval:
        query = (
            query_template
            + f" AND data_report >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {minutes_interval} MINUTE)"
        )
    else:
        query = (
            query_template
            + f" AND data_report BETWEEN TIMESTAMP('{start_datetime}') AND TIMESTAMP('{end_datetime}')"
        )

    # Query execution
    log(f"Executing query with {len(query)} characters")
    df = bd.read_sql(query)

    if df.empty:
        log("No events found in BigQuery", level="warning")
        return pd.DataFrame()  # TODO: end flow run

    log(f"Retrieved {len(df)} raw events from BigQuery")

    return df


@task  # TODO: transform only new events
def task_transform_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and clean events data.

    Args:
        df: Raw events DataFrame

    Returns:
        Cleaned and transformed DataFrame
    """

    # Remove duplicates
    initial_count = len(df)
    df = df[~df["descricao"].duplicated(keep="first")].reset_index(drop=True)
    log(
        f"Removed {initial_count - len(df)} duplicate descriptions, {len(df)} unique events remaining"
    )

    # Analyze address quality
    invalid_addresses = ["n.i.", "não informado", "na", "nan", "none", "n.i. não informado"]

    df_missing_coords = df[
        (df["latitude"] == 0)
        & (~df["logradouro"].astype(str).str.strip().str.lower().isin(invalid_addresses))
    ].copy()

    log(f"Found {len(df_missing_coords)} events with valid addresses but missing coordinates")

    # Add data quality flags
    df["has_coordinates"] = (df["latitude"] != 0) & (df["longitude"] != 0)
    df["has_valid_address"] = (
        ~df["logradouro"].astype(str).str.strip().str.lower().isin(invalid_addresses)
    )
    df["geocodable"] = (~df["has_coordinates"]) & df["has_valid_address"]

    log_msg = (
        f"Data quality summary:\n"
        f"  • Events with coordinates: {df['has_coordinates'].sum()}\n"
        f"  • Events with valid addresses: {df['has_valid_address'].sum()}\n"
        f"  • Events that can be geocoded: {df['geocodable'].sum()}\n"
    )
    log(log_msg)

    # Geocode events
    log(f"Geocoding {len(df_missing_coords)} events")
    df_missing_coords["endereco_busca"] = df_missing_coords.apply(format_address, axis=1)
    df_missing_coords[["latitude", "longitude"]] = df_missing_coords["endereco_busca"].apply(
        lambda e: pd.Series(geocode_address(e))
    )

    df.update(df_missing_coords[["latitude", "longitude"]])

    return df


@task
def task_get_contexts(
    project_id: str = "rj-civitas",
    dataset_id: str = "integracao_reports",
    table_id: str = "contextos",
) -> pd.DataFrame:
    """
    Retrieve monitoring contexts from BigQuery.

    Args:
        project_id: GCP project ID containing the contexts table
        dataset_id: BigQuery dataset ID containing the contexts table
        table_id: BigQuery table ID with monitoring contexts

    Returns:
        DataFrame with monitoring contexts including geometry information
    """
    query = """SELECT
        id,
        tipo,
        datahora_inicio,
        datahora_fim,
        nome,
        descricao,
        informacoes_adicionais,
        endereco,
        local,
        geometria,
        raio_de_busca,
        cidade_inteira,
        solicitante
    FROM `rj-civitas.integracao_reports.contextos`
    """

    log(f"Query to get contexts:\n\n{query}")
    df = bd.read_sql(query)

    if df.empty:
        log("No contexts found in BigQuery", level="warning")
        return pd.DataFrame()  # TODO: end flow run

    log(f"Found {len(df)} contexts")

    df = df.set_index("id")

    return df


@task  # TODO: classify only new events || create hash to identify if the event was already classified
def task_classify_events_public_safety(
    occurrences: pd.DataFrame,
    dspy_config: dict,
    api_key: str | None = None,
    model_name: str = "gemini/gemini-2.5-flash",
    temperature: float = 0.5,
    max_tokens: int = 1024,
    use_threading: bool = False,
    max_workers: int = 10,
) -> pd.DataFrame:
    """
    Classify events for public safety relevance using binary classification.

    Args:
        occurrences: DataFrame with events to classify
        dspy_config: DSPy configuration dictionary (ensures DSPy is configured)
        api_key: Gemini API key
        model_name: LLM model name
        temperature: Generation temperature
        max_tokens: Maximum tokens
        use_threading: Whether to use threading for parallel processing
        max_workers: Number of worker threads

    Returns:
        DataFrame with public safety classification results
    """
    if occurrences.empty:
        log("No occurrences to classify", level="warning")
        return pd.DataFrame()

    # Log that we're using existing DSPy config
    log(f"Using existing DSPy configuration: {dspy_config}")

    # Create public safety classifier using existing DSPy config
    classifier = ClassifierFactory.create_public_safety_classifier(
        use_existing_dspy_config=True,  # Use existing config
        api_key=api_key,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    # Classify events
    results_df = classifier.classify_dataframe(
        occurrences,
        use_threading=use_threading,
        max_workers=max_workers,
        progress_interval=10,
    )

    # Get token logs for analysis
    token_logs_df = classifier.get_token_logs_df()

    if not token_logs_df.empty:
        # Calculate statistics
        total_examples = len(token_logs_df)
        total_tokens = token_logs_df["total_tokens"].sum()
        total_cost = token_logs_df["custo"].sum()

        # Count classifications
        related_count = results_df["is_related"].sum()
        not_related_count = len(results_df) - related_count

        log_msg = (
            f"PUBLIC SAFETY CLASSIFICATION SUMMARY\n"
            f"{'='*50}\n"
            f"Classification Results:\n"
            f"   • Examples analyzed: {total_examples:,}\n"
            f"   • Related to public safety: {related_count:,}\n"
            f"   • Not related to public safety: {not_related_count:,}\n\n"
            f"LLM Usage:\n"
            f"   • Total tokens used: {total_tokens:,}\n"
            f"   • Estimated cost (USD): ${total_cost:.6f}\n\n"
            f"Average per occurrence:\n"
            f"   • Prompt tokens: {token_logs_df['prompt_tokens'].mean():.1f}\n"
            f"   • Completion tokens: {token_logs_df['completion_tokens'].mean():.1f}\n"
            f"   • Total tokens: {token_logs_df['total_tokens'].mean():.1f}\n"
            f"   • Cost per classification: ${token_logs_df['custo'].mean():.6f}\n"
            f"{'='*50}"
        )
        log(log_msg)
    else:
        log("No token usage data available", level="warning")

    # Add id_report column for merging
    results_df["id_report"] = occurrences.reset_index()["id_report"]

    return results_df


@task
def task_classify_events_fixed_categories(
    occurrences: pd.DataFrame,
    api_key: str | None = None,
    model_name: str = "gemini/gemini-2.5-flash",
    temperature: float = 0.5,
    max_tokens: int = 1024,
    fixed_categories: Optional[List[str]] = None,
    use_threading: bool = False,
    max_workers: int = 10,
) -> pd.DataFrame:
    """
    Classify events into fixed categories.

    Args:
        occurrences: DataFrame with events to classify
        api_key: Gemini API key
        model_name: LLM model name
        temperature: Generation temperature
        max_tokens: Maximum tokens
        fixed_categories: List of fixed categories
        use_threading: Whether to use threading for parallel processing
        max_workers: Number of worker threads

    Returns:
        DataFrame with fixed categories classification results
    """
    if occurrences.empty:
        log("No occurrences to classify", level="warning")
        return pd.DataFrame()

    # Create fixed categories classifier
    classifier = ClassifierFactory.create_fixed_categories_classifier(
        api_key=api_key,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        fixed_categories=fixed_categories,
    )

    # Classify events
    results_df = classifier.classify_dataframe(
        occurrences,
        use_threading=use_threading,
        max_workers=max_workers,
        progress_interval=10,
    )

    # Get token logs for analysis
    token_logs_df = classifier.get_token_logs_df()

    if token_logs_df.empty:
        log("No token usage data available", level="warning")
        return pd.DataFrame()

    else:
        # Calculate statistics
        total_examples = len(token_logs_df)
        total_tokens = token_logs_df["total_tokens"].sum()
        total_cost = token_logs_df["custo"].sum()

        # Count categories
        categories_count = results_df.explode("categorias")["categorias"].value_counts()

        log_msg = (
            f"FIXED CATEGORIES CLASSIFICATION SUMMARY\n"
            f"{'='*50}\n"
            f"Classification Results:\n"
            f"   • Examples analyzed: {total_examples:,}\n"
            f"   • Total tokens used: {total_tokens:,}\n"
            f"   • Estimated cost (USD): ${total_cost:.6f}\n\n"
            f"Average per occurrence:\n"
            f"   • Prompt tokens: {token_logs_df['prompt_tokens'].mean():.1f}\n"
            f"   • Completion tokens: {token_logs_df['completion_tokens'].mean():.1f}\n"
            f"   • Total tokens: {token_logs_df['total_tokens'].mean():.1f}\n"
            f"   • Cost per classification: ${token_logs_df['custo'].mean():.6f}\n\n"
            f"Most frequent categories:\n"
            f"{chr(10).join([f'   • {cat}: {count}' for cat, count in categories_count.head(5).items()])}\n"
            f"{'='*50}"
        )
        log(log_msg)

    # Add id_report column for merging
    results_df["id_report"] = occurrences.reset_index()["id_report"]

    return results_df


@task
def task_extract_entities(
    occurrences: pd.DataFrame,
    safety_relevant_events: pd.DataFrame,
    dspy_config: dict,
    api_key: str,
    model_name: str = "gpt-4o",
    temperature: float = 0.7,
    max_tokens: int = 500,
    use_threading: bool = True,
    max_workers: int = 10,
) -> pd.DataFrame:
    """
    Extract entities from event descriptions using LLM-based classification.

    Args:
        occurrences: DataFrame containing event data with columns like 'descricao'
        dspy_config: DSPy configuration dictionary (ensures DSPy is configured)
        api_key: OpenAI API key for LLM access
        model_name: LLM model to use for entity extraction
        temperature: Temperature setting for LLM generation
        max_tokens: Maximum tokens for LLM response
        use_threading: Whether to use threading for parallel processing
        max_workers: Maximum number of threads for processing

    Returns:
        DataFrame with extracted entities including event_types, locations, people, etc.
    """
    log(f"Starting entity extraction for {len(occurrences)} occurrences")

    if occurrences.empty:
        log("No occurrences to process for entity extraction")
        return pd.DataFrame()

    safety_relevant_events = safety_relevant_events[safety_relevant_events["is_related"]]
    if safety_relevant_events.empty:
        log("No safety relevant events found")

    safety_relevant_occurrences = occurrences[
        occurrences["id_report"].isin(safety_relevant_events["id_report"])
    ]
    # Log that we're using existing DSPy config
    log(f"Using existing DSPy configuration: {dspy_config}")

    try:
        # Create entity extraction classifier using existing DSPy config
        classifier = ClassifierFactory.create_entity_extraction_classifier(
            use_existing_dspy_config=True,  # Use existing config
            api_key=api_key,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        # Process the DataFrame
        results_df = classifier.classify_dataframe(
            safety_relevant_occurrences,
            use_threading=use_threading,
            max_workers=max_workers,
            progress_interval=10,
        )

        # Get token logs for analysis
        token_logs_df = classifier.get_token_logs_df()

        if not token_logs_df.empty:
            # Calculate statistics
            total_examples = len(token_logs_df)
            total_tokens = token_logs_df["total_tokens"].sum()
            total_cost = token_logs_df["custo"].sum()

            log_msg = (
                f"ENTITY EXTRACTION SUMMARY\n"
                f"{'='*50}\n"
                f"Processing Results:\n"
                f"   • Examples processed: {total_examples:,}\n"
                f"   • Total tokens used: {total_tokens:,}\n"
                f"   • Estimated cost (USD): ${total_cost:.6f}\n\n"
                f"Average per occurrence:\n"
                f"   • Prompt tokens: {token_logs_df['prompt_tokens'].mean():.1f}\n"
                f"   • Completion tokens: {token_logs_df['completion_tokens'].mean():.1f}\n"
                f"   • Total tokens: {token_logs_df['total_tokens'].mean():.1f}\n"
                f"   • Cost per extraction: ${token_logs_df['custo'].mean():.6f}\n"
                f"{'='*50}"
            )
            log(log_msg)

            # Get extraction statistics
            extraction_stats = classifier.get_extraction_stats(results_df)
            log(f"Extraction statistics: {extraction_stats}")

        else:
            log("No token usage data available", level="warning")

        # Add id_report column for merging
        results_df["id_report"] = occurrences.reset_index()["id_report"]

        log(f"extract_entities df columns: {results_df.columns}")  # TODO: remove this

        selected_columns = [
            "id_report",
            "event_types",
            "locations",
            "people",
            "event_time",
            "reasoning",
            "extraction_type",
        ]
        results_df = results_df[selected_columns]

        return results_df

    except Exception as e:
        log(f"Error creating entity_extraction classifier: {str(e)}", level="error")
        raise


@task
def task_analyze_context_relevance(
    events_df: pd.DataFrame,
    contexts_df: pd.DataFrame,
    df_events_types: pd.DataFrame,
    dspy_config: dict,
    api_key: str,
    model_name: str = "gpt-4o",
    temperature: float = 0.3,
    max_tokens: int = 300,
    use_threading: bool = True,
    max_workers: int = 10,
    raio_buffer: int = 3000,
    prompt_template: str = None,
) -> pd.DataFrame:
    """
    Analyze context relevance for events using LLM-based analysis.

    Args:
        events_df: DataFrame with event data
        contexts_df: DataFrame with context data
        dspy_config: DSPy configuration dictionary (ensures DSPy is configured)
        api_key: OpenAI API key for LLM access
        model_name: LLM model to use
        temperature: Temperature setting for LLM generation
        max_tokens: Maximum tokens for LLM response
        use_threading: Whether to use threading for parallel processing
        max_workers: Maximum number of threads for processing
        raio_buffer: Buffer radius for geographical analysis
        prompt_template: Custom prompt template

    Returns:
        DataFrame with context relevance analysis results
    """
    log(f"Starting context relevance analysis for {len(events_df)} events")

    if events_df.empty:  # TODO: END OR SKIP FLOW RUN
        log("No events to analyze for context relevance")
        return pd.DataFrame()

    if contexts_df.empty:  # TODO: END OR SKIP FLOW RUN
        log("No contexts available for analysis")
        return pd.DataFrame()

    if df_events_types.empty:  # TODO: END OR SKIP FLOW RUN
        log("No events types available for analysis")
        return pd.DataFrame()

    events_df = events_df[events_df["id_report"].isin(df_events_types["id_report"])]
    events_df = pd.merge(events_df, df_events_types, on="id_report", how="left")

    # Log that we're using existing DSPy config
    log(f"Using existing DSPy configuration: {dspy_config}")

    try:
        # Create context relevance classifier using existing DSPy config
        classifier = ClassifierFactory.create_context_relevance_classifier(
            use_existing_dspy_config=True,  # Use existing config
            api_key=api_key,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        # Step 1: Associate events with nearby contexts
        log("Associating events with geographical contexts...")
        enriched_events = associar_contextos_proximos(
            df_eventos=events_df, df_contextos=contexts_df, raio_buffer=raio_buffer
        )

        # Step 2: Generate prompts for relevance analysis
        log("Generating prompts for relevance analysis...")
        prompts_df = gerar_prompts_relevancia(
            df_eventos=enriched_events, df_contextos=contexts_df, prompt_template=prompt_template
        )

        if prompts_df.empty:
            log("No prompts generated for relevance analysis")
            return pd.DataFrame()

        # Step 3: Analyze relevance using LLM
        log(f"Analyzing relevance for {len(prompts_df)} event-context pairs...")
        results_df = classifier.analyze_relevance_dataframe(
            prompts_df,
            use_threading=use_threading,
            max_workers=max_workers,
        )

        log(
            f"Context relevance analysis completed. Analyzed {len(results_df)} event-context pairs."
        )

        return results_df  # TODO: adicionar prompt no df e subir no bq

    except Exception as e:
        log(f"Error in context relevance analysis: {str(e)}", level="error")
        raise


@task
def task_save_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    write_disposition: str = "WRITE_APPEND",
) -> None:
    """
    Save DataFrame to BigQuery.

    Args:
        df: DataFrame to save
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        write_disposition: Write disposition (WRITE_APPEND, WRITE_TRUNCATE)
    """

    if df.empty:
        log("No data to save to BigQuery", level="warning")
        return

    log(f"Saving {len(df)} records to {project_id}.{dataset_id}.{table_id}")

    load_data_from_dataframe(
        dataframe=df,
        dataset_id=dataset_id,
        table_id=table_id,
        project_id=project_id,
        write_disposition=write_disposition,
    )

    log(f"Successfully saved {len(df)} records to BigQuery")


@task
def task_generate_messages(
    events_df: pd.DataFrame,
    context_relevance_df: pd.DataFrame,
    contexts_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate formatted messages from relevant context analysis results.

    Args:
        events_df: DataFrame with event information
        context_relevance_df: DataFrame with context relevance analysis results
        contexts_df: DataFrame with context information

    Returns:
        DataFrame with generated messages for each solicitante and id_report
    """

    if context_relevance_df.empty:
        log("No context relevance data to process for message generation")
        return pd.DataFrame()

    if contexts_df.empty:
        log("No contexts data available for message generation")
        return pd.DataFrame()

    # Filter only relevant events - handle NaN values explicitly
    mask = context_relevance_df["is_relevant"].fillna(False)
    relevant_events = context_relevance_df[mask].copy()

    if relevant_events.empty:
        log("No relevant events found for message generation")
        return pd.DataFrame()

    log(f"Found {len(relevant_events)} relevant event-context pairs for message generation")

    # Reset contexts index to merge properly
    contexts_reset = contexts_df.reset_index()
    # Ensure the index column is named 'contexto_id' for merging
    contexts_reset = contexts_reset.rename(columns={"id": "contexto_id"})

    # Merge relevant events with context information
    df_merged = relevant_events.merge(contexts_reset, on="contexto_id", how="left")
    df_merged = df_merged.merge(events_df, on="id_report", how="left")

    if df_merged.empty:
        log("No data after merging relevant events with contexts")
        return pd.DataFrame()

    # Remove rows where solicitante is null/empty
    df_merged = df_merged.dropna(subset=["solicitante"])
    df_merged = df_merged[df_merged["solicitante"].str.strip() != ""]

    debug_list = []  # TODO: remove this
    for col in df_merged.columns:
        if "descricao" in col:
            debug_list.append(col)
    log(f"columns with 'descricao' in the name: {debug_list}")

    if df_merged.empty:
        log("No valid solicitantes found")
        return pd.DataFrame()

    log(f"Processing messages for {len(df_merged)} solicitante-event combinations")

    mensagens = []
    # Group by (solicitante, id_report) to generate messages
    for (requestor, report_id), grupo in df_merged.groupby(["solicitante", "id_report"]):
        # Get the first row for event data (all rows in group have same event data)
        event_row = grupo.iloc[0]

        # Event Data
        date_event: pd.Timestamp = pd.to_datetime(event_row.get("data_report"))
        delay = get_delay_time_string(date_event=date_event, tz=tz)

        event_type = event_row.get("event_types", ["Ocorrência"])[0]
        risk_level = "Alto Risco" if "tiroteio" in event_type.lower() else "Risco Potencial"

        description = event_row.get("descricao_y", "Não Informado")

        address = event_row.get("logradouro", "") or (
            event_row.get("locations", [""])[0]
            if isinstance(event_row.get("locations"), list)
            else "Não Informado"
        )

        # All related contexts info for this event and this requestor
        contexts_info = ""
        qty_contexts = 0
        contexts_list = []
        for _, ctx in grupo.iterrows():
            name = ctx.get("nome", "Sem nome")
            cid = ctx.get("contexto_id")
            start = pd.to_datetime(ctx.get("datahora_inicio", "")).strftime("%d/%m/%Y %H:%M:%S")
            end = pd.to_datetime(ctx.get("datahora_fim", "")).strftime("%d/%m/%Y %H:%M:%S")
            factor = ctx.get("relevance_reasoning", "Fator não especificado")

            contexts_info += (
                f"🧭 Contexto: {name}\n"
                f"• ID: {cid}\n"
                f"• Vigência: {start} to {end}\n"
                f"• Fator: {factor}\n"
            )
            qty_contexts += 1
            contexts_list.append(name)

        # TODO: add map to message
        # Build final message
        message = (
            f"🧾 Solicitante: {requestor}\n"
            f"🆔 ID do Report: {report_id}\n\n"
            f"➡️ {event_type} - {risk_level}\n"
            f"• Data: {date_event.strftime('%d/%m/%Y %H:%M:%S')}\n"
            f"• Atraso: {delay}\n"
            f"• Endereço: {address}\n"
            f"• Fonte: {event_row.get('id_source', 'Desconhecido')}\n"
            f"• Descrição: {description}\n\n"
            f"📌 Contextos Relacionados:\n\n{contexts_info}\n"
        )

        mensagens.append(
            {
                "solicitante": requestor,
                "id_report": report_id,
                "mensagem": message,
                "timestamp_creation": date_event,
                "contextos_relacionados": contexts_list,
                "tipo_evento": event_type,
                "nivel_risco": risk_level,
                "data_ocorrencia": date_event,
            }
        )

    # Convert to DataFrame
    df_msgs = pd.DataFrame(mensagens)

    log(
        f"Generated {len(df_msgs)} messages for {df_msgs['solicitante'].nunique()} unique solicitantes"
    )

    # Log statistics
    stats_msg = (
        f"MESSAGE GENERATION SUMMARY\n"
        f"{'='*50}\n"
        f"Messages generated: {len(df_msgs)}\n"
        f"Unique solicitantes: {df_msgs['solicitante'].nunique()}\n"
        f"Unique events: {df_msgs['id_report'].nunique()}\n"
        f"{'='*50}"
    )
    log(stats_msg)

    return df_msgs


@task
def task_send_discord_messages(df_messages: pd.DataFrame):
    """
    Send messages to Discord.

    Args:
        df_messages: DataFrame with messages to send
    """

    async def main():
        log("Start sending messages to discord.")
        for _, message in df_messages.iterrows():
            url = os.getenv("DISCORD_WEBHOOK_URL")

            await send_discord_message(
                webhook_url=url,
                message=message.get("mensagem"),
            )

        log("Messages sent to discord successfully.")

    asyncio.run(main())
