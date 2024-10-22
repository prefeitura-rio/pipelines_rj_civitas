# -*- coding: utf-8 -*-
"""
Send a discord alert whenever a new occurrence is detected in the Fogo Cruzado..
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.scraping_redes.telegram.schedules import (
    scraping_telegram_etl_minutely_update_schedule,
)
from pipelines.scraping_redes.telegram.tasks import (
    task_create_pipeline,
    task_fetch_new_messages_from_telegram_channels,
    task_get_secret_folder,
    task_load_to_bigquery,
)

with Flow(
    name="CIVITAS: Scraping Redes - Telegram",
    state_handlers=[handler_initialize_sentry, handler_inject_bd_credentials],
) as scraping_telegram:

    project_id = Parameter("project_id", default="rj-civitas-dev")
    dataset_id = Parameter("dataset_id", default="dev")
    table_id = Parameter("table_id", default="telegram")
    mode = Parameter("mode", default="dev")
    channels_names = Parameter(
        "channels_names", default=["AlertaRio24hr", "madureiranewsrj01", "antigocg2"]
    )
    write_disposition = Parameter("write_disposition", default="WRITE_TRUNCATE")

    redis_secrets = task_get_secret_folder(secret_path="/redis")
    telegram_secrets = task_get_secret_folder(secret_path="/telegram")

    # Initializing Pipeline Class
    pipeline = task_create_pipeline(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        mode=mode,
        channels_names=channels_names,
        redis_secrets=redis_secrets,
        telegram_secrets=telegram_secrets,
    )

    # Search new messagens
    fetch_task = task_fetch_new_messages_from_telegram_channels()
    fetch_task.set_upstream(pipeline)

    # Load new messages to BigQuery
    load_to_bigquery = task_load_to_bigquery()
    load_to_bigquery.set_upstream(fetch_task)


scraping_telegram.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
scraping_telegram.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)
scraping_telegram.schedule = scraping_telegram_etl_minutely_update_schedule
