# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Tasks for execute_dbt
"""

import asyncio
import os
from pathlib import Path

import prefect

# import requests
from dbt.cli.main import dbtRunner, dbtRunnerResult
from prefect.client import Client
from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.infisical import get_flow_run_mode, get_secret
from prefeitura_rio.pipelines_utils.io import get_root_path
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.dbt import Summarizer, log_to_file, process_dbt_logs
from pipelines.utils.notifications import send_discord_message


# TODO: resolver bug de zero width space que quebra o md no Discord.
# Falha está no cabeçaalho da mensagem, ## :x: Erro ao executar DBT ...
# Aparentemente, quando a mensagem é dividida em chunks
@task
def add_dbt_secrets_to_env(dbt_secrets: list[str], path: str = "/") -> dict[str, str]:
    """
    Loads secrets from Infisical and sets them as environment variables.

    """

    if not dbt_secrets:
        log("No dbt secrets provided. Skipping environment variable setup.")
        return {}

    secrets_dict = {}

    for secret_name in dbt_secrets:
        try:
            secret = get_secret(secret_name=secret_name, path=path)
            value = secret[secret_name]
            os.environ[secret_name] = value
            secrets_dict[secret_name] = value
            log(f"Environment variable {secret_name} set successfully.")
        except KeyError:
            log(f"Secret {secret_name} not found in Infisical.")
            continue
        except Exception as e:
            log(f"Error setting environment variable {secret_name}: {e}")
            continue

    return secrets_dict


@task
def execute_dbt(
    repository_path: str = "",
    command: str = "run",
    target: str = "dev",
    select="",
    exclude="",
    state="",
    flag="",
    prefect_environment="",
):
    """
    Executes a dbt command with the specified parameters.

    Args:
        repository_path (str): The path to the dbt repository.
        command (str, optional): The dbt command to execute. Defaults to "run".
        target (str, optional): The dbt target to use. Defaults to "dev".
        select (str, optional): The dbt selector to filter models. Defaults to "".
        exclude (str, optional): The dbt selector to exclude models. Defaults to "".

    Returns:
        dbtRunnerResult: The result of the dbt command execution.
    """
    commands = command.split(" ")

    if not repository_path:
        root_path = get_root_path()
        repository_path = (root_path / "queries").as_posix()

    cli_args = commands + ["--profiles-dir", repository_path, "--project-dir", repository_path]

    if command in ("build", "data_test", "run", "test"):
        cli_args.extend(
            [
                "--target",
                target,
            ]
        )

        if select:
            cli_args.extend(["--select", select])
        if exclude:
            cli_args.extend(["--exclude", exclude])
        if state:
            cli_args.extend(["--state", state])
        if flag:
            cli_args.extend([flag])

        log(f"Executing dbt command: {' '.join(cli_args)}", level="info")

    dbt_runner = dbtRunner()
    running_result: dbtRunnerResult = dbt_runner.invoke(cli_args)

    log_path = os.path.join(repository_path, "logs", "dbt.log")

    log("RESULTADOS:")
    log(running_result)

    environment = get_flow_run_mode()
    secret_name = "DISCORD_WEBHOOK_URL_DBT-RUNS"
    flow_name = prefect.context.get("flow_name")
    flow_run_id = prefect.context.get("flow_run_id")
    webhook_url = get_secret(secret_name=secret_name, environment=environment).get(secret_name)

    if command not in ("deps") and not os.path.exists(log_path):
        message = (
            "## ❌ Erro ao executar DBT\n",
            f"> Prefect Environment: {prefect_environment}\n",
            f"> Flow Run: [{flow_name}](https://pipelines.dados.rio/flow-run/{flow_run_id})\n",
            "> Não foi possível encontrar o arquivo de logs.\n",
        )

        async def main():
            await send_discord_message(
                webhook_url=webhook_url,
                message=message,
            )

        asyncio.run(main())
        raise FAIL("DBT Run seems not successful. No logs found.")

    return running_result


@task
def create_dbt_report(
    running_results: dbtRunnerResult,
    bigquery_project: str,
    repository_path: str = "",
) -> None:
    """
    Creates a report based on the results of running dbt commands.

    Args:
        running_results (dbtRunnerResult): The results of running dbt commands.
        repository_path (str): The path to the repository.

    Raises:
        FAIL: If there are failures in the dbt commands.

    Returns:
        None
    """
    if not repository_path:
        root_path = get_root_path()
        repository_path = (root_path / "queries").as_posix()

    logs = process_dbt_logs(log_path=os.path.join(repository_path, "logs", "dbt.log"))

    log_path = log_to_file(logs)
    summarizer = Summarizer()

    is_successful, has_warnings = True, False

    general_report = []
    failed_models = []
    for command_result in running_results.result:
        if command_result.status == "fail":
            is_successful = False
            general_report.append(f"- 🛑 FAIL: {summarizer(command_result)}")
            failed_models.append(command_result.node.name)
        elif command_result.status == "error":
            is_successful = False
            general_report.append(f"- ❌ ERROR: {summarizer(command_result)}")
            failed_models.append(command_result.node.name)
        elif command_result.status == "warn":
            has_warnings = True
            general_report.append(f"- ⚠️ WARN: {summarizer(command_result)}")
            failed_models.append(command_result.node.name)
        elif command_result.status == "runtime error":  # Table which source freshness failed
            is_successful = False
            general_report.append(f"- ⏱️ STALE TABLE: {summarizer(command_result)}")
            failed_models.append(command_result.node.name)

    # Sort and log the general report
    general_report = sorted(general_report, reverse=True)
    general_report = "**Resumo**:\n" + "  \n".join(
        general_report
    )  # TODO :  * 5 para testar msg maior
    log(general_report)

    # Get Parameters
    param_report = ["**Parametros**:"]

    parameters = prefect.context.get("parameters")
    environment = get_flow_run_mode()

    if environment == "dev":
        bigquery_project = "rj-" + bigquery_project + "-dev"
    elif environment in ["prod", "staging"]:
        bigquery_project = "rj-" + bigquery_project

    param_report.append(f"- Projeto BigQuery: `{bigquery_project}`")
    param_report.append(f"- Target dbt: `{environment}`")
    param_report.append(f"- Comando: `{parameters.get('command')}`")

    if parameters.get("select"):
        param_report.append(f"- Select: `{parameters.get('select')}`")
    if parameters.get("exclude"):
        param_report.append(f"- Exclude: `{parameters.get('exclude')}`")
    if parameters.get("flag"):
        param_report.append(f"- Flag: `{parameters.get('flag')}`")

    param_report.append(
        f"- GitHub Repo: `{parameters.get('github_repo').rsplit('/', 1)[-1].removesuffix('.git')}`"
    )

    param_report = "\n".join(param_report)
    param_report += " \n"

    fully_successful = is_successful and running_results.success
    include_report = has_warnings or (not fully_successful)

    if include_report:
        # DBT - Sending Logs to Discord
        command = prefect.context.get("parameters").get("command")
        emoji = "❌" if not fully_successful else "✅"
        complement = "com Erros" if not fully_successful else "sem Erros"

        secret_name = "DISCORD_WEBHOOK_URL_DBT-RUNS"
        flow_name = prefect.context.get("flow_name")
        flow_run_id = prefect.context.get("flow_run_id")
        webhook_url = get_secret(secret_name=secret_name, environment=environment).get(secret_name)

        title = f"{emoji} [{bigquery_project}] - Execução `dbt {command}` finalizada {complement}"
        header_content = f"""
    ## {title}
    > Prefect Environment: {environment}
    > Flow Run: [{flow_name}](https://pipelines.dados.rio/flow-run/{flow_run_id})
        """
        message = (
            f"{header_content}\n{param_report}\n{general_report}\u200B"
            if include_report
            else param_report
        )  # TODO tinha \u200B depois de general_report

        with open(log_path, "rb") as file:
            file_content = file.read()

        async def main():
            await send_discord_message(
                webhook_url=webhook_url, message=message, file=file_content, file_format="txt"
            )

        asyncio.run(main())

    # Fail the flow if DBT execution was not successful
    if not fully_successful:
        raise FAIL(
            f"DBT execution failed. Check the logs for details. Failed models: {', '.join(failed_models)}"
        )


@task
def rename_current_flow_run_dbt(
    command: str,
    target: str,
    select: str,
    exclude: str,
) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()

    flow_run_name = f"dbt {command}"

    if select:
        flow_run_name += f" --select {select}"
    if exclude:
        flow_run_name += f" --exclude {exclude}"

    flow_run_name += f" --target {target}"

    client.set_flow_run_name(flow_run_id, flow_run_name)
    log(f"Flow run renamed to: {flow_run_name}", level="info")


@task
def get_target_from_environment():
    """
    Retrieves the target environment based on the given environment parameter.
    """
    converter = {
        "prod": "prod",
        "local-prod": "prod",
        "staging": "dev",
        "local-staging": "dev",
        "dev": "dev",
    }
    return converter.get("dev")  # TODO: refactor profiles.yml to support different targets
