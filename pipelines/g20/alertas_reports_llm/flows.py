# -*- coding: utf-8 -*-
"""
G20 - Alerts flow.
"""
from prefect import Flow, Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.g20.alertas_reports_llm.schedules import g20_reports_schedule
from pipelines.g20.alertas_reports_llm.tasks import (
    task_build_messages_text,
    task_get_data,
    task_get_date_execution,
    task_get_llm_reponse_and_update_table,
    task_get_new_alerts,
    task_send_discord_messages,
)
from pipelines.utils.state_handlers import handler_notify_on_failure
from pipelines.utils.tasks import task_get_secret_folder

with Flow(
    name="CIVITAS: G20 - Alertas",
    state_handlers=[
        handler_inject_bd_credentials,
        handler_initialize_sentry,
        handler_notify_on_failure,
    ],
) as g20_alerts:

    project_id = Parameter("project_id", default="rj-civitas")
    dataset_id = Parameter("dataset_id", default="integracao_reports")

    table_id_enriquecido = Parameter("table_id_enriquecido", default="")
    prompt_enriquecimento = Parameter("prompt_enriquecimento", default="")
    query_enriquecimento = Parameter("query_enriquecimento", default="")
    start_datetime_enriquecimento = Parameter("start_datetime_enriquecimento", default=None)
    end_datetime_enriquecimento = Parameter("end_datetime_enriquecimento", default=None)
    minutes_interval_enriquecimento = Parameter("minutes_interval_enriquecimento", default=360)
    get_llm_ocorrencias = Parameter("get_llm_ocorrencias", default=True)

    table_id_relacao = Parameter("table_id_relacao", default="")
    prompt_relacao = Parameter("prompt_relacao", default="")
    query_relacao = Parameter("query_relacao", default="")
    start_datetime_relacao = Parameter("start_datetime_relacao", default=None)
    end_datetime_relacao = Parameter("end_datetime_relacao", default=None)
    minutes_interval_relacao = Parameter("minutes_interval_relacao", default=360)
    get_llm_relacao = Parameter("get_llm_relacao", default=True)

    model_name = Parameter("model_name", default="gemini-1.5-flash-002")
    max_output_tokens = Parameter("max_output_tokens", default=1024)
    temperature = Parameter("temperature", default=0.2)
    top_k = Parameter("top_k", default=32)
    top_p = Parameter("top_p", default=1)
    location = Parameter("location", default="us-central1")
    batch_size = Parameter("batch_size", default=10)

    generate_alerts = Parameter("generate_alerts", default=True)
    table_id_alerts_history = Parameter("table_id_alerts_history", default="")
    minutes_interval_alerts = Parameter("minutes_interval_alerts", default=360)

    date_execution = task_get_date_execution()
    date_execution.set_upstream(batch_size)

    with case(get_llm_ocorrencias, True):

        occurrences = task_get_data(
            source="enriquecimento",
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id_enriquecido,
            query_template=query_enriquecimento,
            prompt=prompt_enriquecimento,
            minutes_interval=minutes_interval_enriquecimento,
            start_datetime=start_datetime_enriquecimento,
            end_datetime=end_datetime_enriquecimento,
        )
        occurrences.set_upstream(date_execution)

        reports_enriquecidos = task_get_llm_reponse_and_update_table(
            dataframe=occurrences,
            dataset_id=dataset_id,
            table_id=table_id_enriquecido,
            model_name=model_name,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            top_k=top_k,
            top_p=top_p,
            project_id=project_id,
            location=location,
            batch_size=batch_size,
            date_execution=date_execution,
            prompt_column="prompt_enriquecimento",
        )

        reports_enriquecidos.set_upstream(occurrences)

    with case(get_llm_relacao, True):

        relations = task_get_data(
            source="relacao",
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id_relacao,
            table_id_enriquecido=table_id_enriquecido,
            query_template=query_relacao,
            prompt=prompt_relacao,
            minutes_interval=minutes_interval_relacao,
            start_datetime=start_datetime_relacao,
            end_datetime=end_datetime_relacao,
        )
        relations.set_upstream(reports_enriquecidos)

        reports_relacao = task_get_llm_reponse_and_update_table(
            dataframe=relations,
            dataset_id=dataset_id,
            table_id=table_id_relacao,
            model_name=model_name,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            top_k=top_k,
            top_p=top_p,
            project_id=project_id,
            location=location,
            batch_size=batch_size,
            date_execution=date_execution,
            prompt_column="prompt_relacao",
        )
        reports_relacao.set_upstream(relations)

    with case(generate_alerts, True):
        secrets = task_get_secret_folder(secret_path="/discord", inject_env=True)
        secrets.set_upstream(reports_relacao)

        new_alerts = task_get_new_alerts(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id_relacao,
            minutes_interval=minutes_interval_alerts,
        )
        new_alerts.set_upstream(secrets)

        messages = task_build_messages_text(
            dataframe=new_alerts,
        )
        messages.set_upstream(new_alerts)

        discord_messages = task_send_discord_messages(
            webhooks=secrets,
            messages=messages,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id_alerts_history,
        )
        discord_messages.set_upstream(messages)


g20_alerts.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
g20_alerts.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

g20_alerts.schedule = g20_reports_schedule
