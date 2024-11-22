# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data
related to 'Disque Denúncia' reports, including updating missing coordinates.
"""


from pathlib import Path

from prefect import Parameter, case
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
from prefeitura_rio.pipelines_utils.tasks import (  # task_run_dbt_model_task,
    create_table_and_upload_to_gcs,
    get_current_flow_project_name,
)

from pipelines.constants import constants
from pipelines.disque_denuncia.extract.schedules import (
    disque_denuncia_etl_minutely_update_schedule,
)
from pipelines.disque_denuncia.extract.tasks import (
    check_report_qty,
    get_reports_from_start_date,
    loop_transform_report_data,
    task_get_date_execution,
    task_get_secret_folder,
    update_missing_coordinates_in_bigquery,
)

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: DataLake - Extração e carga de dados no datalake do Disque Denúncia",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as extracao_disque_denuncia:

    # Parâmetros
    project_id = Parameter("project_id", default="")
    dataset_id = Parameter("dataset_id", default="")
    table_id = Parameter("table_id", default="")
    start_date = Parameter("start_date", default="")
    tipo_difusao = Parameter("tipo_difusao", default="")
    loop_limiter = Parameter("loop_limiter", default=False)
    dump_mode = Parameter("dump_mode", default="")
    biglake_table = Parameter("biglake_table", default=True)
    mod = Parameter("mod", default=100)

    materialize_after_dump = Parameter("materialize_after_dump", default=True)
    dbt_alias = Parameter("dbt_alias", default=False)

    materialize_reports_dd_after_dump = Parameter("materialize_reports_dd_after_dump", default=True)

    # Georeference reports
    georeference_reports = Parameter("georeference_reports", default=True)
    mode = Parameter("mode", default="")
    address_columns = Parameter("address_columns", default=[])
    lat_lon_columns = Parameter("lat_lon_columns", default={})
    id_column_name = Parameter("id_column_name", default="")
    timestamp_creation_column_name = Parameter("timestamp_creation_column_name", default=None)
    start_date_geocoding = Parameter("start_date_geocoding", default=None)
    date_column_name_geocoding = Parameter("date_column_name_geocoding", default=None)

    api_key = task_get_secret_folder(secret_path="/api-keys")
    date_execution = task_get_date_execution(utc=False)

    # Task to get reports from the specified start date
    reports_response = get_reports_from_start_date(
        start_date=start_date,
        file_dir=Path("/tmp/pipelines/disque_denuncia/data/raw"),
        tipo_difusao=tipo_difusao,
        dataset_id=dataset_id,
        table_id=table_id,
        loop_limiter=loop_limiter,
        mod=mod,
    )
    reports_response.set_upstream(table_id)

    # Task to check report quantity
    report_qty_check = check_report_qty(reports_response)
    report_qty_check.set_upstream(reports_response)

    # Extract the list of XML file paths from the reports response
    # Task to transform the XML files into CSV files
    csv_path_list = loop_transform_report_data(
        source_file_path_list=reports_response["xml_file_path_list"],
        final_file_dir=Path("/tmp/pipelines/disque_denuncia/data/partition_directory"),
        mod=mod,
    )
    csv_path_list.set_upstream(report_qty_check)

    # Cria a tabela e faz o upload dos dados para o BigQuery
    create_table = create_table_and_upload_to_gcs(
        data_path=Path("/tmp/pipelines/disque_denuncia/data/partition_directory"),
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )
    create_table.set_upstream(csv_path_list)

    with case(task=materialize_after_dump, value=True):
        # Get TEMPLATE flow name
        materialization_flow_name = settings.FLOW_NAME_EXECUTE_DBT_MODEL
        materialization_labels = task_get_current_flow_run_labels()
        current_flow_project_name = get_current_flow_project_name()

        dump_prod_tables_to_materialize_parameters = [
            {"dataset_id": "disque_denuncia", "table_id": "denuncias", "dbt_alias": False}
        ]

        dump_prod_materialization_flow_runs = create_flow_run.map(
            flow_name=unmapped(materialization_flow_name),
            project_name=unmapped(current_flow_project_name),
            parameters=dump_prod_tables_to_materialize_parameters,
            labels=unmapped(materialization_labels),
        )

        dump_prod_materialization_flow_runs.set_upstream(create_table)

        dump_prod_wait_for_flow_run = wait_for_flow_run.map(
            flow_run_id=dump_prod_materialization_flow_runs,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

        # Run DBT to create/update "reports_disque_denuncia" table in "integracao_reports" dataset
        # Execute only if "materialize_after_dump" is True
        with case(task=materialize_reports_dd_after_dump, value=True):
            reports_dd_tables_to_materialize_parameters = [
                {
                    "dataset_id": "integracao_reports_staging",
                    "table_id": "reports_disque_denuncia",
                    "dbt_alias": False,
                }
            ]

            reports_dd_materialization_flow_runs = create_flow_run.map(
                flow_name=unmapped(
                    "CIVITAS: integracao_reports_staging - Materialize disque denuncia"
                ),
                project_name=unmapped(current_flow_project_name),
                parameters=reports_dd_tables_to_materialize_parameters,
                labels=unmapped(materialization_labels),
            )

            reports_dd_materialization_flow_runs.set_upstream(dump_prod_materialization_flow_runs)

    # Atualiza coordenadas ausentes usando a API do Google Maps
    with case(task=georeference_reports, value=True):
        update_coordinates_task = update_missing_coordinates_in_bigquery(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            id_column_name=id_column_name,
            address_columns_names=address_columns,
            lat_lon_columns_names=lat_lon_columns,
            api_key=api_key["GOOGLE_MAPS_API_KEY"],
            mode=mode,
            date_execution=date_execution,
            start_date=start_date_geocoding,
            date_column_name=date_column_name_geocoding,
            timestamp_creation_column_name=timestamp_creation_column_name,
        )
        update_coordinates_task.set_upstream(dump_prod_materialization_flow_runs)

extracao_disque_denuncia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extracao_disque_denuncia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

extracao_disque_denuncia.schedule = disque_denuncia_etl_minutely_update_schedule
