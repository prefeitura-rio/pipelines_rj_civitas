# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data.
related to 'Disque Denúncia' reports.
"""


from pathlib import Path

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_get_current_flow_run_labels,
    task_get_flow_group_id,
)
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)
from prefeitura_rio.pipelines_utils.tasks import (  # task_run_dbt_model_task,
    create_table_and_upload_to_gcs,
)

from pipelines.constants import constants
from pipelines.disque_denuncia.extract.schedules import (
    disque_denuncia_etl_minutely_update_schedule,
)
from pipelines.disque_denuncia.extract.tasks import (
    check_report_qty,
    get_reports_from_start_date,
    loop_transform_report_data,
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

    start_date = Parameter("start_date", default="2021-01-01")
    dataset_id = Parameter("dataset_id", default="disque_denuncia")
    table_id = Parameter("table_id", default="denuncias")
    dump_mode = Parameter("dump_mode", default="append")
    biglake_table = Parameter("biglake_table", default=True)
    materialize_after_dump = Parameter("materialize_after_dump", default=True)
    materialize_reports_dd_after_dump = Parameter("materialize_reports_dd_after_dump", default=True)
    dbt_alias = Parameter("dbt_alias", default=False)
    loop_limiter = Parameter("loop_limiter", default=False)
    tipo_difusao = Parameter("tipo_difusao", default="interesse")
    mod = Parameter("mod", default=100)

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

    create_table = create_table_and_upload_to_gcs(
        data_path=Path("/tmp/pipelines/disque_denuncia/data/partition_directory"),
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )
    create_table.set_upstream(csv_path_list)

    # Run DBT to create/update "denuncias" table in "disque_denuncia" dataset
    materialization_flow_id = task_get_flow_group_id(
        flow_name=settings.FLOW_NAME_EXECUTE_DBT_MODEL
    )  # verificar .FLOW_NAME
    materialization_labels = task_get_current_flow_run_labels()

    with case(task=materialize_after_dump, value=True):
        dump_prod_tables_to_materialize_parameters = [
            {"dataset_id": "disque_denuncia", "table_id": "denuncias", "dbt_alias": False}
        ]

        dump_prod_materialization_flow_runs = create_flow_run.map(
            flow_id=unmapped(materialization_flow_id),
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
        materialize_reports_dd_flow_id = task_get_flow_group_id(
            flow_name="CIVITAS: integracao_reports_staging - Materialize disque denuncia"
        )

        with case(task=materialize_reports_dd_after_dump, value=True):
            reports_dd_tables_to_materialize_parameters = [
                {
                    "dataset_id": "integracao_reports",
                    "table_id": "reports_disque_denuncia",
                    "dbt_alias": False,
                }
            ]

            reports_dd_materialization_flow_runs = create_flow_run.map(
                flow_id=unmapped(materialize_reports_dd_flow_id),
                parameters=reports_dd_tables_to_materialize_parameters,
                labels=unmapped(materialization_labels),
            )

            reports_dd_materialization_flow_runs.set_upstream(dump_prod_materialization_flow_runs)

            reports_dd_wait_for_flow_run = wait_for_flow_run.map(
                flow_run_id=reports_dd_materialization_flow_runs,
                stream_states=unmapped(True),
                stream_logs=unmapped(True),
                raise_final_state=unmapped(True),
            )

extracao_disque_denuncia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extracao_disque_denuncia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

extracao_disque_denuncia.schedule = disque_denuncia_etl_minutely_update_schedule
