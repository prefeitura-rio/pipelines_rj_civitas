# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT...
"""


from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (  # get_flow_run_mode,
    task_get_current_flow_run_labels,
    task_get_flow_group_id,
)
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants
from pipelines.placas_clonadas.materialize.schedules import (
    placas_clonadas_monthly_update_schedule,
)

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: placas_clonadas - Materialização das tabelas",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as materialize_placas_clonadas:

    # environment = get_flow_run_mode()
    dataset_id = Parameter("dataset_id", default="placas_clonadas")
    # table_id = Parameter("table_id", default="reports")
    # dbt_alias = Parameter("dbt_alias", default=False)

    materialization_flow_id = task_get_flow_group_id(flow_name=settings.FLOW_NAME_EXECUTE_DBT_MODEL)
    materialization_labels = task_get_current_flow_run_labels()

    # dataset_id = dataset_id + "_" + environment if environment != "prod" else dataset_id

    dump_prod_tables_to_materialize_parameters = [
        # {"dataset_id": dataset_id, "table_id": "radares_confiaveis", "dbt_alias": False},
        # {"dataset_id": dataset_id, "table_id": "dados_radares", "dbt_alias": False},
        # {"dataset_id": dataset_id, "table_id": "identificacao_anomalias_placas", "dbt_alias": False},
        {"dataset_id": dataset_id, "table_id": "placas_clonadas", "dbt_alias": False},
    ]

    dump_prod_materialization_flow_runs = create_flow_run.map(
        flow_id=unmapped(materialization_flow_id),
        parameters=dump_prod_tables_to_materialize_parameters,
        labels=unmapped(materialization_labels),
    )

    dump_prod_wait_for_flow_run = wait_for_flow_run.map(
        flow_run_id=dump_prod_materialization_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

materialize_placas_clonadas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_placas_clonadas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

materialize_placas_clonadas.schedule = placas_clonadas_monthly_update_schedule
