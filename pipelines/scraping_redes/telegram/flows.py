# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data.
"""

from prefect import Parameter  # case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.custom import Flow

# from prefeitura_rio.pipelines_utils.prefect import (
# task_get_current_flow_run_labels,
# task_rename_current_flow_run_dataset_table,
# )
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants
from pipelines.scraping_redes.telegram.schedules import telegram_update_schedule
from pipelines.scraping_redes.telegram.tasks import (
    task_get_channels_names_from_bq,
    task_get_chats,
    task_get_messages,
    task_get_secret_folder,
    task_load_to_table,
    task_set_palver_variables,
)

# from prefeitura_rio.pipelines_utils.tasks import get_current_flow_project_name


# Define the Prefect Flow for data extraction and transformation
with Flow(
    name="CIVITAS: Telegram (PALVER) - Extração e Carga",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_skip_if_running,
    ],
) as extracao_palver_telegram:

    project_id = Parameter("project_id", default="")
    dataset_id = Parameter("dataset_id", default="")
    dataset_id_staging = Parameter("dataset_id_staging", default="")
    table_id_usuarios = Parameter("table_id_usuarios", default="")
    table_id_messages = Parameter("table_id_messages", default="")
    table_id_chats = Parameter("table_id_chats", default="")
    write_disposition_chats = Parameter("write_disposition_chats", default="")
    write_disposition_messages = Parameter("write_disposition_messages", default="")
    start_date = Parameter("start_date", default=None)
    end_date = Parameter("end_date", default=None)
    # materialize_after_dump = Parameter("materialize_after_dump", default=True)
    # materialize_reports_fc_after_dump = Parameter("materialize_reports_fc_after_dump", default=True)

    secrets = task_get_secret_folder(secret_path="/palver")
    # redis_password = task_get_secret_folder(secret_path="/redis")

    palver_variables = task_set_palver_variables(
        base_url=secrets["PALVER_BASE_URL"],
        token=secrets["PALVER_TOKEN"],
    )

    channels_names = task_get_channels_names_from_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id_usuarios,
    )
    channels_names.set_upstream(palver_variables)

    chats = task_get_chats(
        chat_usernames=channels_names,
    )
    chats.set_upstream(channels_names)

    load_chats_to_bq = task_load_to_table(
        project_id=project_id,
        dataset_id=dataset_id_staging,
        table_id=table_id_chats,
        occurrences=chats,
        write_disposition=write_disposition_chats,
    )
    load_chats_to_bq.set_upstream(chats)

    messages = task_get_messages(
        chats=chats,
        start_date=start_date,
        end_date=end_date,
    )
    messages.set_upstream(chats)

    load_messages_to_bq = task_load_to_table(
        project_id=project_id,
        dataset_id=dataset_id_staging,
        table_id=table_id_messages,
        occurrences=messages,
        write_disposition=write_disposition_messages,
    )
    load_messages_to_bq.set_upstream(messages)

extracao_palver_telegram.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extracao_palver_telegram.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

extracao_palver_telegram.schedule = telegram_update_schedule
