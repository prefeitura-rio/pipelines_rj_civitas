# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_templates.run_dbt_model.flows import (
    templates__run_dbt_model__flow,
)
from prefeitura_rio.pipelines_utils.prefect import set_default_parameters
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.disque_denuncia.materialize.schedules import (
    civitas_disque_denuncia_datalake_hourly_update_schedule,
)

run_dbt_civitas_disque_denuncia_datalake_flow = deepcopy(templates__run_dbt_model__flow)
run_dbt_civitas_disque_denuncia_datalake_flow.state_handlers = [handler_inject_bd_credentials]

run_dbt_civitas_disque_denuncia_datalake_flow.name = (
    "CIVITAS: Datalake Disque Denúncia - Materializar tabelas"
)
run_dbt_civitas_disque_denuncia_datalake_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_civitas_disque_denuncia_datalake_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

civitas_disque_denuncia_datalake_default_parameters = {
    "dataset_id": "disque_denuncia",
    "upstream": True,
    "materialize_to_datario": False,
}
run_dbt_civitas_disque_denuncia_datalake_flow = set_default_parameters(
    run_dbt_civitas_disque_denuncia_datalake_flow,
    default_parameters=civitas_disque_denuncia_datalake_default_parameters,
)

run_dbt_civitas_disque_denuncia_datalake_flow.schedule = (
    civitas_disque_denuncia_datalake_hourly_update_schedule
)
