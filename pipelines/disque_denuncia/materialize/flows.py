# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for transforming data.
related to 'Disque Denúncia' historic reports.
"""

# from prefect import Parameter, case
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
)

from pipelines.constants import constants

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: DataLake - Materialização de tabelas no datalake do Disque Denúncia",
    state_handlers=[handler_inject_bd_credentials, handler_initialize_sentry],
) as materialize_disque_denuncia:

    # dataset_id = Parameter("dataset_id", default="disque_denuncia")
    # table_id = Parameter("table_id", default="denuncias")
    # dbt_alias = Parameter("dbt_alias", default=False)

    materialization_flow_id = task_get_flow_group_id(flow_name=settings.FLOW_NAME_EXECUTE_DBT_MODEL)
    materialization_labels = task_get_current_flow_run_labels()

    dump_prod_tables_to_materialize_parameters = [
        {"dataset_id": "disque_denuncia", "table_id": "denuncias", "dbt_alias": False},
    ]

    dump_prod_materialization_hist_flow_runs = create_flow_run.map(
        flow_id=unmapped(materialization_flow_id),
        parameters=dump_prod_tables_to_materialize_parameters,
        labels=unmapped(materialization_labels),
    )

    dump_prod_wait_for_flow_run = wait_for_flow_run.map(
        flow_run_id=dump_prod_materialization_hist_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

materialize_disque_denuncia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_disque_denuncia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)
