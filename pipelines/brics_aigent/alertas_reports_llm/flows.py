# -*- coding: utf-8 -*-
"""
BRICS - Alerts flow.....
"""

# from dotenv import load_dotenv  # TODO: remover
from prefect import Flow, Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.brics_aigent.alertas_reports_llm.schedules import brics_reports_schedule
from pipelines.brics_aigent.alertas_reports_llm.tasks import (  # task_classify_events_fixed_categories,
    task_analyze_context_relevance,
    task_classify_events_public_safety,
    task_configure_dspy,
    task_extract_entities,
    task_generate_messages,
    task_get_contexts,
    task_get_date_execution,
    task_get_events,
    task_save_to_bigquery,
    task_send_discord_messages,
    task_transform_events,
)
from pipelines.constants import constants
from pipelines.utils.state_handlers import handler_notify_on_failure
from pipelines.utils.tasks import task_get_secret_folder

with Flow(
    name="CIVITAS: BRICS - Alertas",
    state_handlers=[
        handler_skip_if_running,
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_notify_on_failure,
    ],
) as brics_alerts:

    # Data source parameters
    source_project_id = Parameter("source_project_id", default=None)
    source_dataset_id = Parameter("source_dataset_id", default=None)
    source_table_id = Parameter("source_table_id", default=None)

    # Data destination parameters
    # TODO: change to default="rj-civitas"
    destination_project_id = Parameter("destination_project_id", default=None)
    destination_dataset_id = Parameter("destination_dataset_id", default=None)

    classified_events_safety_table_id = Parameter("classified_events_safety_table_id", default=None)
    classified_events_categories_table_id = Parameter(
        "classified_events_categories_table_id", default=None
    )
    extracted_entities_table_id = Parameter("extracted_entities_table_id", default=None)
    context_relevance_table_id = Parameter("context_relevance_table_id", default=None)
    messages_table_id = Parameter("messages_table_id", default=None)

    # Time filtering parameters
    start_datetime = Parameter("start_datetime", default=None)
    end_datetime = Parameter("end_datetime", default=None)
    minutes_interval = Parameter("minutes_interval", default=None)

    # Query parameters
    query_events = Parameter("query_events", default="")

    # Classification type selection
    use_public_safety_classification = Parameter("use_public_safety_classification", default=True)
    use_fixed_categories_classification = Parameter(
        "use_fixed_categories_classification", default=False
    )
    use_entity_extraction = Parameter("use_entity_extraction", default=True)
    use_context_relevance_analysis = Parameter("use_context_relevance_analysis", default=True)

    # LLM parameters
    model_name = Parameter("model_name", default=None)
    temperature = Parameter("temperature", default=None)
    max_tokens = Parameter("max_tokens", default=None)

    prompt_context_relevance = Parameter("prompt_context_relevance", default=None)

    # Threading parameters
    use_threading = Parameter("use_threading", default=True)
    max_workers = Parameter("max_workers", default=10)

    # Fixed categories parameters (for fixed categories classification)
    fixed_categories = Parameter("fixed_categories", default=None)

    # Processing parameters
    batch_size = Parameter("batch_size", default=10)

    date_execution = task_get_date_execution()
    date_execution.set_upstream(batch_size)

    # messages parameters
    send_context_relevance_messages = Parameter("send_context_relevance_messages", default=True)

    # Carregar variáveis do .env
    # load_dotenv()  # TODO: remover

    # Injetar secrets do Infisical
    task_get_secret_folder(
        secret_path="/aigents", inject_env=True
    )  # TODO: trocar o environment para o ambiente correto

    # with case(get_llm_ocorrencias, True):

    # Get raw events
    raw_events = task_get_events(
        query_template=query_events,
        minutes_interval=minutes_interval,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )
    raw_events.set_upstream(date_execution)

    # Get contexts
    contexts = task_get_contexts()
    contexts.set_upstream(date_execution)

    # Transform events
    clean_events = task_transform_events(raw_events)
    clean_events.set_upstream(raw_events)

    # Configure DSPy once at the beginning
    dspy_config = task_configure_dspy(
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    # Public Safety Classification
    with case(use_public_safety_classification, True):
        classified_events_safety = task_classify_events_public_safety(
            occurrences=clean_events,
            dspy_config=dspy_config,  # Pass the config
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            destination_project_id=destination_project_id,
            destination_dataset_id=destination_dataset_id,
            destination_table_id=classified_events_safety_table_id,
            use_threading=use_threading,
            max_workers=max_workers,
        )
        classified_events_safety.set_dependencies(upstream_tasks=[clean_events, dspy_config])

        # Save public safety results to BigQuery
        save_safety_result = task_save_to_bigquery(
            df=classified_events_safety,
            project_id=destination_project_id,
            dataset_id=destination_dataset_id,
            table_id=classified_events_safety_table_id,
        )
        save_safety_result.set_upstream(classified_events_safety)

    # Fixed Categories Classification
    # with case(use_fixed_categories_classification, True):
    #     classified_events_categories = task_classify_events_fixed_categories(
    #         occurrences=clean_events,
    #         model_name=model_name,
    #         temperature=temperature,
    #         max_tokens=max_tokens,
    #         fixed_categories=fixed_categories,
    #         use_threading=use_threading,
    #         max_workers=max_workers,
    #     )
    #     classified_events_categories.set_upstream(classified_events_safety)

    #     # Save fixed categories results to BigQuery
    #     save_categories_result = task_save_to_bigquery(
    #         df=classified_events_categories,
    #         project_id=destination_project_id,
    #         dataset_id=destination_dataset_id,
    #         table_id="eventos_classificados_categorias_fixas",
    #     )
    #     save_categories_result.set_upstream(classified_events_categories)

    # Entity Extraction
    with case(use_entity_extraction, True):
        extracted_entities = task_extract_entities(
            occurrences=clean_events,
            safety_relevant_events=classified_events_safety,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            use_threading=use_threading,
            max_workers=max_workers,
        )
        extracted_entities.set_dependencies(upstream_tasks=[classified_events_safety, dspy_config])

        # Save entity extraction results to BigQuery
        save_entities_result = task_save_to_bigquery(
            df=extracted_entities,
            project_id=destination_project_id,
            dataset_id=destination_dataset_id,
            table_id=extracted_entities_table_id,
        )
        save_entities_result.set_upstream(extracted_entities)

    # Context relevance analysis (conditional and requires entity extraction)
    with case(use_context_relevance_analysis, True):
        context_relevance_results = task_analyze_context_relevance(
            events_df=clean_events,  # Use clean_events which has latitude/longitude for geographical analysis
            contexts_df=contexts,
            df_events_types=extracted_entities,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            use_threading=use_threading,
            max_workers=max_workers,
            prompt_template=prompt_context_relevance,
        )
        context_relevance_results.set_dependencies(upstream_tasks=[extracted_entities, contexts])

        # Save context relevance results to BigQuery
        save_context_relevance_result = task_save_to_bigquery(
            df=context_relevance_results,
            project_id=destination_project_id,
            dataset_id=destination_dataset_id,
            table_id=context_relevance_table_id,
        )
        save_context_relevance_result.set_upstream(context_relevance_results)

    # Generate messages from relevant events
    with case(send_context_relevance_messages, True):
        messages = task_generate_messages(
            events_df=clean_events,
            context_relevance_df=context_relevance_results,
            contexts_df=contexts,
            destination_project_id=destination_project_id,
            destination_dataset_id=destination_dataset_id,
            destination_table_id=messages_table_id,
        )
        messages.set_upstream(context_relevance_results)

        # Save messages to BigQuery
        save_messages_result = task_save_to_bigquery(
            df=messages,
            project_id=destination_project_id,
            dataset_id=destination_dataset_id,
            table_id=messages_table_id,
        )
        save_messages_result.set_upstream(messages)

        # Send messages to Discord
        # TODO: save messages after sending to Discord and sinalize if it was sent successfully
        messages_sent = task_send_discord_messages(df_messages=messages)
        messages_sent.set_upstream(save_messages_result)

brics_alerts.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
brics_alerts.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

# from prefect.executors import LocalDaskExecutor  # TODO: remover

# brics_alerts.executor = LocalDaskExecutor(num_workers=1)
brics_alerts.schedule = brics_reports_schedule
