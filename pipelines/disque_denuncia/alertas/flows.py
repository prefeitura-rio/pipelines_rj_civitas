# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for sending alerts to Discord.
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants

# from pipelines.disque_denuncia.alertas.schedules import (
#     alertas_disque_denuncia_etl_minutely_update_schedule,
# )
from pipelines.disque_denuncia.alertas.tasks import (
    task_create_messages,
    task_get_new_reports,
    task_send_discord_messages,
    task_update_last_report_datetime,
)
from pipelines.utils.state_handlers import handler_notify_on_failure
from pipelines.utils.tasks import task_get_secret_folder

# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: Disque Denúncia - Alertas",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
        handler_notify_on_failure,
    ],
) as alertas_disque_denuncia:

    project_id = Parameter("project_id", default="")
    dataset_id = Parameter("dataset_id", default="")
    table_id = Parameter("table_id", default="")
    start_datetime = Parameter("start_datetime", default="")
    keywords = Parameter("keywords", default=[])

    redis_secret = task_get_secret_folder(secret_path="/redis", inject_env=True)
    secrets = task_get_secret_folder(secret_path="/discord", inject_env=True)

    new_reports = task_get_new_reports(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        start_datetime=start_datetime,
        keywords=keywords,
    )

    messages = task_create_messages(reports_response=new_reports)
    messages.set_upstream(new_reports)

    send_discord_message = task_send_discord_messages(messages=messages)
    send_discord_message.set_upstream(messages)

    update_last_report_datetime = task_update_last_report_datetime(
        dataset_id=dataset_id,
        table_id=table_id,
    )
    update_last_report_datetime.set_upstream(send_discord_message)

alertas_disque_denuncia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
alertas_disque_denuncia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

# alertas_disque_denuncia.schedule = alertas_disque_denuncia_etl_minutely_update_schedule
