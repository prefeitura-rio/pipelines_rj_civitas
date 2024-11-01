# -*- coding: utf-8 -*-
"""
G20 - Alerts flow
"""
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.g20.dbt_run_relatorio_enriquecido.schedules import g20_reports_schedule
from pipelines.g20.dbt_run_relatorio_enriquecido.tasks import (
    task_build_messages_text,
    task_get_secret_folder,
    task_query_data_from_sql_file,
    task_send_discord_messages,
    task_skip_flow_run,
)

with Flow(
    name="CIVITAS: G20 - Alertas",
    state_handlers=[handler_inject_bd_credentials, handler_initialize_sentry],
) as g20_alerts:

    data_set_id = Parameter("data_set_id", default="g20")
    table_id = Parameter("table_id", default="reports_enriquecidos_filtrados")

    secrets: dict = task_get_secret_folder(secret_path="/discord")

    data = task_query_data_from_sql_file(model_dataset_id=data_set_id, model_table_id=table_id)

    messages = task_build_messages_text(df=data)
    messages.set_upstream(data)

    check_response = task_skip_flow_run(messages)
    check_response.set_upstream(messages)

    messages_discord = task_send_discord_messages(url_webhook=secrets["G20"], messages=messages)
    messages_discord.set_upstream(check_response)

g20_alerts.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
g20_alerts.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

g20_alerts.schedule = g20_reports_schedule
