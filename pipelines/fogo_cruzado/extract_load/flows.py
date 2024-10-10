# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data...
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_get_current_flow_run_labels,
    task_rename_current_flow_run_dataset_table,
)
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)
from prefeitura_rio.pipelines_utils.tasks import get_current_flow_project_name

from pipelines.constants import constants
from pipelines.fogo_cruzado.extract_load.schedules import (
    fogo_cruzado_etl_update_schedule,
)
from pipelines.fogo_cruzado.extract_load.tasks import (
    check_report_qty,
    fetch_occurrences,
    get_current_timestamp,
    load_to_table,
    task_check_max_document_number,
    task_get_secret_folder,
    task_update_max_document_number_on_redis,
)

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: Fogo Cruzado - Extração e Carga",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as extracao_fogo_cruzado:

    start_date = Parameter("start_date", default=None)
    project_id = Parameter("project_id", default="rj-civitas")
    dataset_id = Parameter("dataset_id", default="fogo_cruzado")
    table_id = Parameter("table_id", default="ocorrencias")
    write_disposition = Parameter("write_disposition", default="WRITE_TRUNCATE")
    materialize_after_dump = Parameter("materialize_after_dump", default=True)
    materialize_reports_fc_after_dump = Parameter("materialize_reports_fc_after_dump", default=True)
    prefix = Parameter("prefix", default="FULL_REFRESH_")
    send_discord_alerts = Parameter("send_discord_alerts", default=True)

    secrets = task_get_secret_folder(secret_path="/api-fogo-cruzado")
    redis_password = task_get_secret_folder(secret_path="/redis")

    # Rename current flow run to identify if is full refresh or partial
    task_rename_current_flow_run_dataset_table(
        prefix=prefix, dataset_id=dataset_id, table_id=table_id
    )

    # Task to get reports from the specified start date
    occurrences_reponse = fetch_occurrences(
        email=secrets["FOGOCRUZADO_USERNAME"],
        password=secrets["FOGOCRUZADO_PASSWORD"],
        initial_date=start_date,
    )

    # Task to check report quantity
    report_qty_check = check_report_qty(task_response=occurrences_reponse)
    report_qty_check.set_upstream(occurrences_reponse)

    # Task to check if there are any new occurrences
    max_document_number_check = task_check_max_document_number(
        occurrences=occurrences_reponse,
        dataset_id=dataset_id,
        table_id=table_id,
        prefix=prefix,
        redis_password=redis_password["REDIS_PASSWORD"],
    )
    max_document_number_check.set_upstream(report_qty_check)

    start_timestamp = get_current_timestamp()
    start_timestamp.set_upstream(max_document_number_check)

    load_to_table_response = load_to_table(
        project_id=project_id,
        dataset_id=dataset_id + "_staging",
        table_id=table_id,
        occurrences=occurrences_reponse,
        write_disposition=write_disposition,
    )

    load_to_table_response.set_upstream(start_timestamp)

    with case(task=materialize_after_dump, value=True):
        materialization_labels = task_get_current_flow_run_labels()

        materialization_flow_name = settings.FLOW_NAME_EXECUTE_DBT_MODEL
        dump_prod_tables_to_materialize_parameters = [
            {"dataset_id": dataset_id, "table_id": table_id, "dbt_alias": False}
        ]
        current_flow_project_name = get_current_flow_project_name()

        dump_prod_materialization_flow_runs = create_flow_run.map(
            flow_name=unmapped(materialization_flow_name),
            project_name=unmapped(current_flow_project_name),
            parameters=dump_prod_tables_to_materialize_parameters,
            labels=unmapped(materialization_labels),
        )

        dump_prod_materialization_flow_runs.set_upstream(load_to_table_response)

        dump_prod_wait_for_flow_run = wait_for_flow_run.map(
            flow_run_id=dump_prod_materialization_flow_runs,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

        update_max_document_number_on_redis = task_update_max_document_number_on_redis(
            new_document_number=max_document_number_check,
            dataset_id=dataset_id,
            table_id=table_id,
            redis_password=redis_password["REDIS_PASSWORD"],
        )
        update_max_document_number_on_redis.set_upstream(dump_prod_wait_for_flow_run)

        with case(task=send_discord_alerts, value=True):
            alerta_discord_parameters = [
                {
                    "start_datetime": start_timestamp,
                    "webhook_url": secrets,
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

        # Execute only if "materialize_after_dump" is True
        with case(task=materialize_reports_fc_after_dump, value=True):
            reports_fc_tables_to_materialize_parameters = [
                {
                    "dataset_id": "integracao_reports_staging",
                    "table_id": "reports_fogo_cruzado",
                    "dbt_alias": False,
                }
            ]

            reports_fc_materialization_flow_runs = create_flow_run.map(
                flow_name=unmapped(
                    "CIVITAS: integracao_reports_staging - Materialize fogo cruzado"
                ),
                project_name=unmapped(current_flow_project_name),
                parameters=reports_fc_tables_to_materialize_parameters,
                labels=unmapped(materialization_labels),
            )

            reports_fc_materialization_flow_runs.set_upstream(update_max_document_number_on_redis)

extracao_fogo_cruzado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extracao_fogo_cruzado.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

extracao_fogo_cruzado.schedule = fogo_cruzado_etl_update_schedule
