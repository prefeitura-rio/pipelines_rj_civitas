# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data.
"""

from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_get_current_flow_run_labels,
    task_rename_current_flow_run_dataset_table,
)
from prefeitura_rio.pipelines_utils.state_handlers import (  # handler_initialize_sentry,
    handler_skip_if_running,
)
from prefeitura_rio.pipelines_utils.tasks import get_current_flow_project_name

from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE, constants
from pipelines.fogo_cruzado.extract_load.schedules import (
    fogo_cruzado_etl_update_schedule,
)
from pipelines.fogo_cruzado.extract_load.tasks import (
    check_report_qty,
    fetch_occurrences,
    get_current_timestamp,
    load_to_table,
    task_check_max_document_number,
    task_update_max_document_number_on_redis,
)
from pipelines.utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_notify_on_failure,
)
from pipelines.utils.tasks import task_get_secret_folder

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: Fogo Cruzado - Extração e Carga",
    state_handlers=[
        handler_inject_bd_credentials,
        # handler_initialize_sentry,
        handler_skip_if_running,
        handler_notify_on_failure,
    ],
) as extracao_fogo_cruzado:

    #####################################
    # Parameters
    #####################################

    # Extraction
    PREFIX = Parameter("prefix", default="FULL_REFRESH_")
    START_DATE = Parameter("start_date", default=None)
    TAKE = Parameter("take", default=100)

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=True)
    SEND_DISCORD_ALERTS = Parameter(
        "send_discord_alerts", default=True
    )  # TODO: remove this parameter
    SEND_DISCORD_REPORT = Parameter("send_discord_report", default=True)

    # DBT
    COMMAND = Parameter("command", default=None)
    SELECT = Parameter("select", default=None)
    GITHUB_REPO = Parameter("github_repo", default=None)
    BIGQUERY_PROJECT = Parameter("bigquery_project", default=None)
    DBT_SECRETS = Parameter("dbt_secrets", default=[])

    # Tables
    PROJECT_ID = Parameter("project_id", default="rj-civitas")
    DATASET_ID = Parameter("dataset_id", default="fogo_cruzado")
    TABLE_ID = Parameter("table_id", default="ocorrencias")
    WRITE_DISPOSITION = Parameter("write_disposition", default="WRITE_TRUNCATE")

    # Materialization
    MATERIALIZE_AFTER_DUMP = Parameter("materialize_after_dump", default=True)
    MATERIALIZE_REPORTS_FC_AFTER_DUMP = Parameter("materialize_reports_fc_after_dump", default=True)

    # Secrets
    DISCORD_SECRETS = task_get_secret_folder(secret_path="/discord", inject_env=True)
    SECRETS = task_get_secret_folder(secret_path="/api-fogo-cruzado")
    REDIS_PASSWORD = task_get_secret_folder(secret_path="/redis")

    # Rename current flow run to identify if is full refresh or partial
    rename_flow_run = task_rename_current_flow_run_dataset_table(
        prefix=PREFIX, dataset_id=DATASET_ID, table_id=TABLE_ID
    )

    #####################################
    # EXTRACT DATA
    #####################################

    # Task to get reports from the specified start date
    occurrences_reponse = fetch_occurrences(
        email=SECRETS["FOGOCRUZADO_USERNAME"],
        password=SECRETS["FOGOCRUZADO_PASSWORD"],
        initial_date=START_DATE,
        take=TAKE,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        redis_password=REDIS_PASSWORD["REDIS_PASSWORD"],
    )

    # Task to check report quantity
    report_qty_check = check_report_qty(task_response=occurrences_reponse)
    report_qty_check.set_upstream(occurrences_reponse)

    # Task to check if there are any new occurrences
    max_document_number_check = task_check_max_document_number(
        occurrences=occurrences_reponse,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        prefix=PREFIX,
        redis_password=REDIS_PASSWORD["REDIS_PASSWORD"],
    )
    max_document_number_check.set_upstream(report_qty_check)

    start_timestamp = get_current_timestamp()
    start_timestamp.set_upstream(max_document_number_check)

    #####################################
    # LOAD RAW DATA
    #####################################

    load_to_table_response = load_to_table(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID + "_staging",
        table_id=TABLE_ID,
        occurrences=occurrences_reponse,
        write_disposition=WRITE_DISPOSITION,
    )

    load_to_table_response.set_upstream(start_timestamp)

    #####################################
    # MATERIALIZE DATA
    #####################################

    with case(task=MATERIALIZE_AFTER_DUMP, value=True):
        materialization_labels = task_get_current_flow_run_labels()

        #####################################
        # Create Flow Runs
        #####################################
        materialization_flow_name = constants.FLOW_NAME_DBT_TRANSFORM.value
        current_flow_project_name = get_current_flow_project_name()

        materialization_parameters = [
            {
                "select": DATASET_ID,
                "github_repo": GITHUB_REPO,
                "bigquery_project": BIGQUERY_PROJECT,
                "dbt_secrets": DBT_SECRETS,
            },
        ]

        materialization_flow_runs = create_flow_run.map(
            flow_name=unmapped(materialization_flow_name),
            project_name=unmapped(current_flow_project_name),
            parameters=materialization_parameters,
            labels=unmapped(materialization_labels),
        )
        materialization_flow_runs.set_upstream(load_to_table_response)

        materialization_wait_for_flow_run = wait_for_flow_run.map(
            flow_run_id=materialization_flow_runs,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

        update_max_document_number_on_redis = task_update_max_document_number_on_redis(
            new_document_number=max_document_number_check,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            redis_password=REDIS_PASSWORD["REDIS_PASSWORD"],
        )
        update_max_document_number_on_redis.set_upstream(materialization_wait_for_flow_run)

        #####################################
        # SEND DISCORD ALERTS FOR RECENT OCCURRENCES
        #####################################

        with case(task=SEND_DISCORD_ALERTS, value=True):
            alerta_discord_parameters = [
                {
                    "start_datetime": start_timestamp,
                    "reasons": ["disputa"],
                }
            ]

            alerta_discord_flow_runs = create_flow_run.map(
                flow_name=unmapped("CIVITAS: ALERTA DISCORD - Fogo Cruzado"),
                project_name=unmapped(current_flow_project_name),
                parameters=alerta_discord_parameters,
                labels=unmapped(materialization_labels),
            )
            alerta_discord_flow_runs.set_upstream(update_max_document_number_on_redis)

        #####################################
        # MATERIALIZE REPORTS FOGO CRUZADO
        #####################################

        # Execute only if "materialize_after_dump" is True
        with case(task=MATERIALIZE_REPORTS_FC_AFTER_DUMP, value=True):
            materialization_parameters = [
                {
                    "rename_flow": RENAME_FLOW,
                    "send_discord_report": SEND_DISCORD_REPORT,
                    "command": "build",
                    "select": "integracao_reports_staging.reports_fogo_cruzado",
                    "github_repo": GITHUB_REPO,
                    "bigquery_project": BIGQUERY_PROJECT,
                    "dbt_secrets": DBT_SECRETS,
                },
            ]

            reports_fc_materialization_flow_runs = create_flow_run.map(
                flow_name=unmapped(materialization_flow_name),
                project_name=unmapped(current_flow_project_name),
                parameters=materialization_parameters,
                labels=unmapped(materialization_labels),
            )
            reports_fc_materialization_flow_runs.set_upstream(update_max_document_number_on_redis)

extracao_fogo_cruzado.storage = FLOW_STORAGE
extracao_fogo_cruzado.run_config = FLOW_RUN_CONFIG

extracao_fogo_cruzado.schedule = fogo_cruzado_etl_update_schedule
