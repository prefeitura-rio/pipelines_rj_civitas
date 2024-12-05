# -*- coding: utf-8 -*-
"""
Send a discord alert whenever a new occurrence is detected in the Fogo Cruzado.
"""
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.dev.dev.tasks import my_task, task_get_secret_folder
from pipelines.dev.dev.utils import notify_on_failure

with Flow(
    name="state-handler-demo",
    state_handlers=[notify_on_failure],
) as flow:

    secrets = task_get_secret_folder(secret_path="/discord")

    fail_task = my_task()

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)
