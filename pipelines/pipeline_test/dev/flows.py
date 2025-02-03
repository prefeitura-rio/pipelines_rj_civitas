# -*- coding: utf-8 -*-
"""
This module imports functions and classes from the 'pipelines.pipeline_test.dev'.
module.
"""
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.pipeline_test.dev.tasks import (
    task_inject_env,
    task_that_fails,
    task_that_fails_2,
    task_that_succeeds,
)
from pipelines.utils.state_handlers import handler_notify_on_failure

with Flow(
    name="CIVITAS: example-flow",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_notify_on_failure,
    ],
) as test_flow:

    infisical_secret_name = Parameter("infisical_secret_name", default="PIPELINES_RESULTS")
    infisical_environment = Parameter("infisical_environment", default="staging")
    infisical_secret_path = Parameter("infisical_secret_path", default="/discord/")
    infisical_inject_env = Parameter("infisical_inject_env", default=True)

    secrets = task_inject_env(
        secret_name=infisical_secret_name,
        environment=infisical_environment,
        secret_path=infisical_secret_path,
        inject_env=infisical_inject_env,
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
