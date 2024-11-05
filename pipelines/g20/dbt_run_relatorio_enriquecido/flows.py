# -*- coding: utf-8 -*-
"""
G20 - Alerts flow
"""
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
# from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.g20.dbt_run_relatorio_enriquecido.schedules import g20_reports_schedule
from pipelines.g20.dbt_run_relatorio_enriquecido.tasks import (
    task_get_date_execution,
    task_get_occurrences,
    task_update_dados_enriquecidos_table,
)

# from prefeitura_rio.core import settings
# from prefeitura_rio.pipelines_utils.prefect import task_get_current_flow_run_labels
# from prefeitura_rio.pipelines_utils.tasks import (
#     get_current_flow_project_name
# )


with Flow(
    name="CIVITAS: G20 - Alertas",
    state_handlers=[handler_inject_bd_credentials, handler_initialize_sentry],
) as g20_alerts:

    project_id = Parameter("project_id", default="rj-civitas")
    dataset_id = Parameter("dataset_id", default="integracao_reports")
    table_id_enriquecido = Parameter("table_id_enriquecido", default="reports_enriquecidos")
    prompt_enriquecimento = Parameter("prompt_enriquecimento", default="")
    query_enriquecimento = Parameter("query_enriquecimento", default="")
    start_datetime = Parameter("start_datetime", default=None)
    end_datetime = Parameter("end_datetime", default=None)
    minutes_interval = Parameter("minutes_interval", default=30)

    table_id_correlacao = Parameter("table_id", default="reports_contexto_enriquecidos")

    model_name = Parameter("model_name", default="gemini-1.5-flash-001")
    max_output_tokens = Parameter("max_output_tokens", default=1024)
    temperature = Parameter("temperature", default=0.2)
    top_k = Parameter("top_k", default=32)
    top_p = Parameter("top_p", default=1)
    location = Parameter("location", default="us-central1")
    batch_size = Parameter("batch_size", default=10)

    # secrets: dict = task_get_secret_folder(secret_path="/discord")

    # # Get TEMPLATE flow name
    # materialization_flow_name = settings.FLOW_NAME_EXECUTE_DBT_MODEL
    # materialization_labels = task_get_current_flow_run_labels()
    # current_flow_project_name = get_current_flow_project_name()

    # dump_prod_tables_to_materialize_parameters = [
    #     {"dataset_id": dataset_id, "table_id": table_id, "dbt_alias": False}
    # ]

    # dump_prod_materialization_flow_runs = create_flow_run.map(
    #     flow_name=unmapped(materialization_flow_name),
    #     project_name=unmapped(current_flow_project_name),
    #     parameters=dump_prod_tables_to_materialize_parameters,
    #     labels=unmapped(materialization_labels),
    # )

    # dump_prod_materialization_flow_runs.set_upstream(secrets)

    # dump_prod_wait_for_flow_run = wait_for_flow_run.map(
    #     flow_run_id=dump_prod_materialization_flow_runs,
    #     stream_states=unmapped(True),
    #     stream_logs=unmapped(True),
    #     raise_final_state=unmapped(True),
    # )

    date_execution = task_get_date_execution()
    date_execution.set_upstream(batch_size)

    occurrences = task_get_occurrences(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id_enriquecido,
        query_enriquecimento=query_enriquecimento,
        prompt_enriquecimento=prompt_enriquecimento,
        minutes_interval=minutes_interval,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )
    occurrences.set_upstream(date_execution)

    reports_enriquecidos = task_update_dados_enriquecidos_table(
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
    )

    reports_enriquecidos.set_upstream(occurrences)

    # reports_enriquecidos_exists = task_check_if_table_exists(dataset_id=dataset_id, table_id='reports_enriquecidos')
    # data = task_query_data_from_sql_file(model_dataset_id=dataset_id, model_table_id='reports_enriquecidos_v2', minutes_ago=10)

#     messages = task_build_messages_text(df=data)
#     messages.set_upstream(data)

#     check_response = task_skip_flow_run(messages)
#     check_response.set_upstream(messages)

#     messages_discord = task_send_discord_messages(url_webhook=secrets["G20"], messages=messages)
#     messages_discord.set_upstream(check_response)

g20_alerts.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
g20_alerts.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CIVITAS_AGENT_LABEL.value,
    ],
)

g20_alerts.schedule = g20_reports_schedule
