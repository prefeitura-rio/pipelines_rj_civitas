# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data
related to 'Disque Denúncia' reports, including updating missing coordinates.
"""


from pathlib import Path

from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import task_get_current_flow_run_labels
from prefeitura_rio.pipelines_utils.state_handlers import (  # handler_initialize_sentry,
    handler_skip_if_running,
)
from prefeitura_rio.pipelines_utils.tasks import (  # task_run_dbt_model_task,
    create_table_and_upload_to_gcs,
    get_current_flow_project_name,
)

from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE, constants
from pipelines.disque_denuncia.extract.schedules import (
    disque_denuncia_etl_minutely_update_schedule,
)
from pipelines.disque_denuncia.extract.tasks import (
    check_report_qty,
    get_reports_from_start_date,
    loop_transform_report_data,
    task_get_date_execution,
    update_missing_coordinates_in_bigquery,
)
from pipelines.utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_notify_on_failure,
)
from pipelines.utils.tasks import task_get_secret_folder

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: DataLake - Extração e carga de dados no datalake do Disque Denúncia",
    state_handlers=[
        handler_inject_bd_credentials,
        # handler_initialize_sentry,
        handler_skip_if_running,
        handler_notify_on_failure,
    ],
) as extracao_disque_denuncia:

    # Parâmetros
    PROJECT_ID = Parameter("project_id", default="")
    DATASET_ID = Parameter("dataset_id", default="")
    TABLE_ID = Parameter("table_id", default="")
    START_DATE = Parameter("start_date", default="")
    TIPO_DIFUSAO = Parameter("tipo_difusao", default="")
    LOOP_LIMITER = Parameter("loop_limiter", default=False)
    DUMP_MODE = Parameter("dump_mode", default="")
    BIGLAKE_TABLE = Parameter("biglake_table", default=True)
    MOD = Parameter("mod", default=100)

    MATERIALIZE_AFTER_DUMP = Parameter("materialize_after_dump", default=True)
    DBT_ALIAS = Parameter("dbt_alias", default=False)

    MATERIALIZE_REPORTS_DD_AFTER_DUMP = Parameter("materialize_reports_dd_after_dump", default=True)

    # Georeference reports
    GEOREFERENCE_REPORTS = Parameter("georeference_reports", default=True)
    MODE = Parameter("mode", default="")
    ADDRESS_COLUMNS = Parameter("address_columns", default=[])
    LAT_LON_COLUMNS = Parameter("lat_lon_columns", default={})
    ID_COLUMN_NAME = Parameter("id_column_name", default="")
    TIMESTAMP_CREATION_COLUMN_NAME = Parameter("timestamp_creation_column_name", default=None)
    START_DATE_GEOCODING = Parameter("start_date_geocoding", default=None)
    DATE_COLUMN_NAME_GEOCODING = Parameter("date_column_name_geocoding", default=None)

    api_key = task_get_secret_folder(secret_path="/api-keys")
    secrets = task_get_secret_folder(secret_path="/discord", inject_env=True)

    date_execution = task_get_date_execution(utc=False)

    # Task to get reports from the specified start date
    reports_response = get_reports_from_start_date(
        start_date=START_DATE,
        file_dir=Path("/tmp/pipelines/disque_denuncia/data/raw"),
        tipo_difusao=TIPO_DIFUSAO,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        loop_limiter=LOOP_LIMITER,
        mod=MOD,
    )
    reports_response.set_upstream(TABLE_ID)
    # Task to check report quantity
    report_qty_check = check_report_qty(reports_response)
    report_qty_check.set_upstream(reports_response)

    # Extract the list of XML file paths from the reports response
    # Task to transform the XML files into CSV files
    csv_path_list = loop_transform_report_data(
        source_file_path_list=reports_response["xml_file_path_list"],
        final_file_dir=Path("/tmp/pipelines/disque_denuncia/data/partition_directory"),
        mod=MOD,
    )
    csv_path_list.set_upstream(report_qty_check)

    # Cria a tabela e faz o upload dos dados para o BigQuery
    create_table = create_table_and_upload_to_gcs(
        data_path=Path("/tmp/pipelines/disque_denuncia/data/partition_directory"),
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_mode=DUMP_MODE,
        biglake_table=BIGLAKE_TABLE,
    )
    create_table.set_upstream(csv_path_list)

    with case(task=MATERIALIZE_AFTER_DUMP, value=True):
        # Get TEMPLATE flow name
        materialization_flow_name = constants.FLOW_NAME_DBT_TRANSFORM.value
        materialization_labels = task_get_current_flow_run_labels()
        current_flow_project_name = get_current_flow_project_name()

        dump_prod_tables_to_materialize_parameters = [
            {
                "select": TABLE_ID,
            }
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
        with case(task=MATERIALIZE_REPORTS_DD_AFTER_DUMP, value=True):
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
    with case(task=GEOREFERENCE_REPORTS, value=True):
        update_coordinates_task = update_missing_coordinates_in_bigquery(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            id_column_name=ID_COLUMN_NAME,
            address_columns_names=ADDRESS_COLUMNS,
            lat_lon_columns_names=LAT_LON_COLUMNS,
            api_key=api_key["GOOGLE_MAPS_API_KEY"],
            mode=MODE,
            date_execution=date_execution,
            start_date=START_DATE_GEOCODING,
            date_column_name=DATE_COLUMN_NAME_GEOCODING,
            timestamp_creation_column_name=TIMESTAMP_CREATION_COLUMN_NAME,
        )
        update_coordinates_task.set_upstream(dump_prod_materialization_flow_runs)

extracao_disque_denuncia.storage = FLOW_STORAGE
extracao_disque_denuncia.run_config = FLOW_RUN_CONFIG

extracao_disque_denuncia.schedule = disque_denuncia_etl_minutely_update_schedule
