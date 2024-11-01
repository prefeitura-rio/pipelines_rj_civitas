# -*- coding: utf-8 -*-
"""
DBT-related flows..
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_templates.run_dbt_model.flows import (
    templates__run_dbt_model__flow,
)
from prefeitura_rio.pipelines_utils.prefect import set_default_parameters
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.g20.dbt_run_relatorio_enriquecido.schedules import g20_reports_schedule

g20_reports_run_dbt_flow = deepcopy(templates__run_dbt_model__flow)
g20_reports_run_dbt_flow.name = "CIVITAS: G20 - Relatório Enriquecido - Materializar tabelas"
g20_reports_run_dbt_flow.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
g20_reports_run_dbt_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
g20_reports_run_dbt_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_CIVITAS_AGENT_LABEL.value],
)

g20_default_parameters = {
    "dataset_id": "g20",
    "upstream": False,
    "dbt_alias": False,
}
g20_reports_run_dbt_flow = set_default_parameters(
    g20_reports_run_dbt_flow,
    default_parameters=g20_default_parameters,
)

g20_reports_run_dbt_flow.schedule = g20_reports_schedule
