# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data
related to 'Disque Denúncia' reports.
"""


from pathlib import Path

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)
from prefeitura_rio.pipelines_utils.tasks import (
    create_table_and_upload_to_gcs,
    task_run_dbt_model_task,
)

from pipelines.constants import constants
from pipelines.reports_disque_denuncia.extract.schedules import (
    disque_denuncia_etl_hour_update_schedule,
)
from pipelines.reports_disque_denuncia.extract.tasks import (
    get_reports_from_start_date,
    loop_transform_report_data,
)

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: DataLake - Extração e carga de dados no datalake do Disque Denúncia",
    state_handlers=[handler_inject_bd_credentials, handler_initialize_sentry],
) as extracao_disque_denuncia:

    start_date = Parameter("start_date", default="2021-01-01")
    dataset_id = Parameter("dataset_id", default="disque_denuncia")
    table_id = Parameter("table_id", default="denuncias")
    dump_mode = Parameter("dump_mode", default="append")
    biglake_table = Parameter("biglake_table", default=True)
    materialize_after_dump = Parameter("materialize_after_dump", default=False)
    dbt_alias = Parameter("dbt_alias", default=False)
    loop_limiter = Parameter("loop_limiter", default=False)
    tipo_difusao = Parameter("tipo_difusao", default="interesse")

    # Task to get reports from the specified start date
    reports_response = get_reports_from_start_date(
        start_date=start_date,
        file_dir=Path("/tmp/pipelines/reports_disque_denuncia/data/raw"),
        tipo_difusao=tipo_difusao,
        dataset_id=dataset_id,
        table_id=table_id,
        loop_limiter=loop_limiter,
    )
    reports_response.set_upstream(table_id)

    # Extract the list of XML file paths from the reports response
    # Task to transform the XML files into CSV files
    csv_path_list = loop_transform_report_data(
        source_file_path_list=reports_response["xml_file_path_list"],
        final_file_dir=Path("/tmp/pipelines/reports_disque_denuncia/data/partition_directory"),
    )
    csv_path_list.set_upstream(reports_response)

    create_table = create_table_and_upload_to_gcs(
        data_path=Path("/tmp/pipelines/reports_disque_denuncia/data/partition_directory"),
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )
    create_table.set_upstream(csv_path_list)

    with case(task=materialize_after_dump, value=True):
        task_run_dbt_model_task(dataset_id=dataset_id, table_id=table_id, dbt_alias=dbt_alias)
        task_run_dbt_model_task.set_upstream(create_table)

extracao_disque_denuncia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extracao_disque_denuncia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

extracao_disque_denuncia.schedule = disque_denuncia_etl_hour_update_schedule
