# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for materializing tables using DBT.
related to 'disque_denuncia.denuncias'...
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
)
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)
from prefeitura_rio.pipelines_utils.tasks import (  # task_run_dbt_model_task,
    get_current_flow_project_name,
)

from pipelines.constants import constants

# from pipelines.integracao_reports_staging.materialize_disque_denuncia.schedules import (
#     integracao_reports_disque_denuncia_minutely_update_schedule,
# )

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: integracao_reports_staging - Materialize disque denuncia",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as materialize_integracao_reports_disque_denuncia:

    dataset_id = Parameter("dataset_id", default="integracao_reports_staging")
    table_id = Parameter("table_id", default="reports_disque_denuncia")
    dbt_alias = Parameter("dbt_alias", default=False)

    materialization_flow_name = settings.FLOW_NAME_EXECUTE_DBT_MODEL
    materialization_labels = task_get_current_flow_run_labels()

    dump_prod_tables_to_materialize_parameters = [
        {"dataset_id": dataset_id, "table_id": table_id, "dbt_alias": dbt_alias}
    ]
    current_flow_project_name = get_current_flow_project_name()

    dump_prod_materialization_flow_runs = create_flow_run.map(
        flow_name=unmapped(materialization_flow_name),
        project_name=unmapped(current_flow_project_name),
        parameters=dump_prod_tables_to_materialize_parameters,
        labels=unmapped(materialization_labels),
    )

    dump_prod_wait_for_flow_run = wait_for_flow_run.map(
        flow_run_id=dump_prod_materialization_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

materialize_integracao_reports_disque_denuncia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_integracao_reports_disque_denuncia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

# materialize_integracao_reports_disque_denuncia.schedule = (
#     integracao_reports_disque_denuncia_minutely_update_schedule
# )
