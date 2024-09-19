# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT.
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
from pipelines.radar_readings.materialize.schedules import (
    radar_readings_twice_daily_update_schedule,
)

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: radar_readings - Materialização das tabelas",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as materialize_radar_readings:

    # environment = get_flow_run_mode()
    dataset_id = Parameter("dataset_id", default="radar_readings")
    # table_id = Parameter("table_id", default="reports")
    # dbt_alias = Parameter("dbt_alias", default=False)

    materialization_flow_id = task_get_flow_group_id(flow_name=settings.FLOW_NAME_EXECUTE_DBT_MODEL)
    materialization_labels = task_get_current_flow_run_labels()

    # dataset_id = dataset_id + "_" + environment if environment != "prod" else dataset_id

    dump_prod_tables_to_materialize_parameters = [
        {"dataset_id": dataset_id, "table_id": "previous_next_readings", "dbt_alias": False},
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

materialize_radar_readings.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_radar_readings.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

materialize_radar_readings.schedule = radar_readings_twice_daily_update_schedule
