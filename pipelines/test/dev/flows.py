# -*- coding: utf-8 -*-
from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.test.dev.tasks import (
    task_inject_env,
    task_that_fails,
    task_that_fails_2,
    task_that_succeeds,
)
from pipelines.test.dev.utils import handler_notify_on_failure

with Flow(
    name="CIVITAS: example-flow",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_notify_on_failure,
    ],
) as test_flow:
    secrets = task_inject_env(
        secret_name="PIPELINES_RESULTS",
        environment="staging",
        secret_path="/discord/",
        inject_env=True,
    )

    success = task_that_succeeds()
    success.set_upstream(secrets)

    fail = task_that_fails()
    fail.set_upstream(secrets)

    fail_2 = task_that_fails_2()
    fail_2.set_upstream(secrets)

test_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
test_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)
