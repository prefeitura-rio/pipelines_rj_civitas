# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data..
"""

from prefect import Parameter, case  # case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import task_get_current_flow_run_labels
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)
from prefeitura_rio.pipelines_utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_project_name,
)

from pipelines.constants import constants
from pipelines.scraping_redes.twitter.schedules import twitter_update_schedule
from pipelines.scraping_redes.twitter.tasks import (
    task_geocode_localities,
    task_get_channels_names_from_bq,
    task_get_chats,
    task_get_date_execution,
    task_get_llm_reponse_and_update_table,
    task_get_messages,
    task_get_secret_folder,
    task_load_to_table,
    task_save_geocoded_data,
    task_set_palver_variables,
)

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: Twitter (PALVER) - Extração e Carga",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as extracao_palver_twitter:

    project_id = Parameter("project_id", default="")
    dataset_id = Parameter("dataset_id", default="")
    table_id = Parameter("table_id", default="")
    table_id_usuarios = Parameter("table_id_usuarios", default="")
    table_id_messages = Parameter("table_id_messages", default="")
    table_id_chats = Parameter("table_id_chats", default="")
    table_id_enriquecido = Parameter("table_id_enriquecido", default="")
    table_id_georreferenciado = Parameter("table_id_georreferenciado", default="")
    dump_mode_chats = Parameter("dump_mode_chats", default="")
    write_disposition_messages = Parameter("write_disposition_messages", default="")
    start_date = Parameter("start_date", default=None)
    end_date = Parameter("end_date", default=None)
    mode = Parameter("mode", default="")
    query_enriquecimento = Parameter("query_enriquecimento", default="")

    model_name = Parameter("model_name", default="gemini-1.5-flash-002")
    max_output_tokens = Parameter("max_output_tokens", default=1024)
    temperature = Parameter("temperature", default=0.2)
    top_k = Parameter("top_k", default=32)
    top_p = Parameter("top_p", default=1)
    location = Parameter("location", default="us-central1")
    batch_size = Parameter("batch_size", default=10)

    twitter_raw_chats_path = Parameter(
        "twitter_raw_chats_path", default="/tmp/pipelines/scraping_redes/x/data/raw/chats"
    )

    materialize_after_dump = Parameter("materialize_after_dump", default=True)
    materialize_reports_twitter_after_dump = Parameter(
        "materialize_reports_twitter_after_dump", default=True
    )

    secrets = task_get_secret_folder(secret_path="/palver")
    api_key = task_get_secret_folder(secret_path="/api-keys")
    # import os

    # secrets = {
    #     "PALVER_BASE_URL": os.getenv("PALVER_BASE_URL"),
    #     "PALVER_TOKEN": os.getenv("PALVER_TOKEN"),
    # }
    # api_key = {"GOOGLE_MAPS_API_KEY": os.getenv("GOOGLE_MAPS_API_KEY")}
    # redis_password = task_get_secret_folder(secret_path="/redis")

    date_execution = task_get_date_execution(utc=True)

    palver_variables = task_set_palver_variables(
        base_url=secrets["PALVER_BASE_URL"],
        token=secrets["PALVER_TOKEN"],
    )

    channels_names = task_get_channels_names_from_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id_usuarios,
    )
    channels_names.set_upstream(palver_variables)

    chats = task_get_chats(
        destination_path=twitter_raw_chats_path,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id_chats,
        chat_usernames=channels_names,
        mode=mode,
    )
    chats.set_upstream(channels_names)

    load_chats_to_bq = create_table_and_upload_to_gcs(
        data_path=twitter_raw_chats_path,
        dataset_id=dataset_id,
        table_id=table_id_chats,
        dump_mode=dump_mode_chats,
        biglake_table=False,
    )
    load_chats_to_bq.set_upstream(chats)

    messages = task_get_messages(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id_messages,
        start_date=start_date,
        end_date=end_date,
        mode=mode,
    )
    messages.set_upstream(load_chats_to_bq)

    load_messages_to_bq = task_load_to_table(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id_messages,
        occurrences=messages,
        write_disposition=write_disposition_messages,
        mode=mode,
    )
    load_messages_to_bq.set_upstream(messages)

    task_enriquecimento = task_get_llm_reponse_and_update_table(
        dataset_id=dataset_id,
        table_id=table_id_enriquecido,
        query=query_enriquecimento,
        model_name=model_name,
        max_output_tokens=max_output_tokens,
        temperature=temperature,
        top_k=top_k,
        top_p=top_p,
        project_id=project_id,
        location=location,
        batch_size=batch_size,
        date_execution=date_execution,
        prompt_column="prompt_column",
        mode=mode,
    )
    task_enriquecimento.set_upstream(load_messages_to_bq)

    geocoded_data = task_geocode_localities(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id_enriquecido,
        date_execution=date_execution,
        mode=mode,
        api_key=api_key["GOOGLE_MAPS_API_KEY"],
    )
    geocoded_data.set_upstream(task_enriquecimento)

    save_geocoded = task_save_geocoded_data(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id_georreferenciado,
        geocoded_data=geocoded_data,
        mode=mode,
    )
    save_geocoded.set_upstream(geocoded_data)

    with case(task=materialize_after_dump, value=True):

        materialization_flow_name = settings.FLOW_NAME_EXECUTE_DBT_MODEL
        materialization_labels = task_get_current_flow_run_labels()
        current_flow_project_name = get_current_flow_project_name()

        twitter_materialization_parameters = [
            {
                "dataset_id": dataset_id,
                "table_id": table_id,
                "dbt_alias": False,
            }
        ]

        twitter_materialization_flow_runs = create_flow_run.map(
            flow_name=unmapped(materialization_flow_name),
            project_name=unmapped(current_flow_project_name),
            parameters=twitter_materialization_parameters,
            labels=unmapped(materialization_labels),
        )

        twitter_materialization_flow_runs.set_upstream(save_geocoded)

        twitter_materialization_wait_for_flow_run = wait_for_flow_run.map(
            flow_run_id=twitter_materialization_flow_runs,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

        with case(task=materialize_reports_twitter_after_dump, value=True):
            twitter_reports_materialization_parameters = [
                {
                    "dataset_id": "integracao_reports_staging",
                    "table_id": "reports_twitter",
                    "dbt_alias": False,
                }
            ]

            twitter_reports_materialization_flow_runs = create_flow_run.map(
                flow_name=unmapped("CIVITAS: integracao_reports_staging - Materialize twitter"),
                project_name=unmapped(current_flow_project_name),
                parameters=twitter_reports_materialization_parameters,
                labels=unmapped(materialization_labels),
            )
            twitter_reports_materialization_flow_runs.set_upstream(
                twitter_materialization_flow_runs
            )


extracao_palver_twitter.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extracao_palver_twitter.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

extracao_palver_twitter.schedule = twitter_update_schedule
