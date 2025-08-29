# -*- coding: utf-8 -*-
import re
from os import getenv
from pathlib import Path
from sys import executable
from typing import Dict, List, Union

try:
    from prefect.tasks.dbt.dbt import DbtShellTask
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])

import pandas as pd
from dbt.contracts.results import RunResult, SourceFreshnessResult
from prefeitura_rio.pipelines_utils.io import get_root_path
from prefeitura_rio.pipelines_utils.logging import log


def run_dbt_model(
    dataset_id: str = None,
    table_id: str = None,
    dbt_alias: bool = False,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
):
    """
    Runs a DBT model.

    Args:
        dataset_id (str): Dataset ID of the dbt model.
        table_id (str, optional): Table ID of the dbt model. If None, the
        whole dataset will be run.
        dbt_alias (bool, optional): If True, the model will be run by
        its alias. Defaults to False.
        upstream (bool, optional): If True, the upstream models will be run.
        downstream (bool, optional): If True, the downstream models will
        be run.
        exclude (str, optional): Models to exclude from the run.
        flags (str, optional): Flags to pass to the dbt run command.
        See:
        https://docs.getdbt.com/reference/dbt-jinja-functions/flags/
        _vars (Union[dict, List[Dict]], optional): Variables to pass to
        dbt. Defaults to None.
    """
    # Set models and upstream/downstream for dbt

    log(f"RUNNING DBT MODEL: {dataset_id}.{table_id}\nDBT_ALIAS: {dbt_alias}")

    run_command = "dbt run --select "

    if upstream:
        run_command += "+"

    if table_id:
        if dbt_alias:
            table_id = f"{dataset_id}.{dataset_id}__{table_id}"
        else:
            table_id = f"{dataset_id}.{table_id}"
    else:
        table_id = dataset_id

    run_command += f"{table_id}"

    if downstream:
        run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"

    if flags:
        run_command += f" {flags}"

    log(f"Running dbt with command: {run_command}")
    root_path = get_root_path()
    queries_dir = str(root_path / "queries")
    dbt_task = DbtShellTask(
        profiles_dir=queries_dir,
        helper_script=f"cd {queries_dir}",
        log_stderr=True,
        return_all=True,
        command=run_command,
    )

    # Add current Python executable's path to the PATH environment variable
    # to make sure we can find the dbt executable
    current_path = getenv("PATH")
    executable_path = Path(executable).parent
    new_path = f"{current_path}:{executable_path}"
    dbt_logs = dbt_task.run(env={"PATH": new_path})

    log("\n".join(dbt_logs))


def get_basic_treated_query(table):
    """
    generates a basic treated query
    """

    originais = table["original_name"].tolist()
    nomes = table["name"].tolist()
    tipos = table["type"].tolist()

    project_id = table["project_id"].unique()[0]
    dataset_id = table["dataset_id"].unique()[0]
    dataset_id = dataset_id.replace("_staging", "")
    table_id = table["table_id"].unique()[0]

    indent_space = 4 * " "
    query = "SELECT \n"
    for original, nome, tipo in zip(originais, nomes, tipos):
        if tipo == "GEOGRAPHY":
            query += indent_space + f"ST_GEOGFROMTEXT({original}) AS {nome},\n"
        elif "id_" in nome or tipo == "INT64":
            query += (
                indent_space
                + f"SAFE_CAST(REGEXP_REPLACE(TRIM({original}), r'\.0$', '') AS {tipo}) AS {nome},\n"  # noqa
            )
        elif tipo == "DATETIME":
            query += (
                indent_space
                + f"SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', {original}) AS {tipo}) AS {nome},\n"  # noqa
            )
        elif tipo == "DATE":
            query += indent_space + f"SAFE_CAST(DATE({original}) AS {tipo}) AS {nome},\n"
        elif tipo == "FLOAT64":
            query += (
                indent_space
                + f"SAFE_CAST(REGEXP_REPLACE({original}, r',', '.') AS {tipo}) AS {nome},\n"
            )
        else:
            query += indent_space + f"SAFE_CAST(TRIM({original}) AS {tipo}) AS {nome},\n"

    query += f"FROM `{project_id}.{dataset_id}_staging.{table_id}` AS t"

    return query


def generate_basic_treated_queries(dataframe, save=False):
    """
    generates a basic treated queries
    """

    cols_to_rename = {
        "project_id": "project_id",
        "dataset_id": "dataset_id",
        "table_id_x": "table_id",
        "column_name": "original_name",
        "nome_da_coluna": "name",
        "tipo_da_coluna": "type",
    }
    dataframe = dataframe[list(cols_to_rename.keys())]
    dataframe = dataframe.rename(columns=cols_to_rename)
    for table_id in dataframe["table_id"].unique().tolist():
        table = dataframe[dataframe["table_id"] == table_id]
        query = get_basic_treated_query(table)
        print(query)
        print("\n\n")
        if save:
            with open(f"./{table_id}.sql", "w") as f:
                f.write(query)


def process_dbt_logs(log_path: str = "dbt_repository/logs/dbt.log") -> pd.DataFrame:
    """
    Process the contents of a dbt log file and return a DataFrame containing the parsed log entries

    Args:
        log_path (str): The path to the dbt log file. Defaults to "dbt_repository/logs/dbt.log".

    Returns:
        pd.DataFrame: A DataFrame containing the parsed log entries.
    """

    with open(log_path, "r", encoding="utf-8", errors="ignore") as log_file:
        log_content = log_file.read()

    result = re.split(r"(\x1b\[0m\d{2}:\d{2}:\d{2}\.\d{6})", log_content)
    parts = [part.strip() for part in result][1:]

    splitted_log = []
    for i in range(0, len(parts), 2):
        time = parts[i].replace(r"\x1b[0m", "")
        level = parts[i + 1][1:6].replace(" ", "")
        text = parts[i + 1][7:]
        splitted_log.append((time, level, text))

    full_logs = pd.DataFrame(splitted_log, columns=["time", "level", "text"])

    return full_logs


def log_to_file(logs: pd.DataFrame, levels=None) -> str:
    """
    Writes the logs to a file and returns the file path.

    Args:
        logs (pd.DataFrame): The logs to be written to the file.
        levels (list): The levels of logs to be written to the file.

    Returns:
        str: The file path of the generated log file.
    """
    if levels is None:
        levels = ["info", "error", "warn"]
    logs = logs[logs.level.isin(levels)]

    report = []
    for _, row in logs.iterrows():
        report.append(f"{row['time']} [{row['level'].rjust(5, ' ')}] {row['text']}")
    report = "\n".join(report)
    log(f"Logs do DBT:{report}")

    target_path = Path(__file__).parents[2] / "tmp" / "dbt_log.txt"
    with open(target_path, "w+", encoding="utf-8") as log_file:
        log_file.write(report)

    return target_path.as_posix()


def extract_table_info_from_compiled_code(compiled_code):
    """Extract database, schema and table from the compiled_code of the test"""

    # Padrão para capturar: `database`.`schema`.`tabela`
    pattern = r"`([^`]+)`\.`([^`]+)`\.`([^`]+)`"

    match = re.search(pattern, compiled_code)
    if match:
        database = match.group(1)
        schema = match.group(2)
        tabela = match.group(3)

        return {
            "database": database,
            "schema": schema,
            "tabela": tabela,
            "full_name": f"{database}.{schema}.{tabela}",
        }

    return None


# =============================
# SUMMARIZERS
# =============================


class RunResultSummarizer:
    """
    A class that summarizes the result of a DBT run.

    Methods:
    - summarize(result): Summarizes the result based on its status.
    - error(result): Returns an error message for the given result.
    - fail(result): Returns a fail message for the given result.
    - warn(result): Returns a warning message for the given result.
    """

    def summarize(self, result: RunResult):
        if result.status == "error":
            return self.error(result)
        elif result.status == "fail":
            return self.fail(result)
        elif result.status == "warn":
            return self.warn(result)

    def error(self, result: RunResult):
        return f"`{result.node.name}`\n  {result.message.replace('__', '_')}  \n"

    # def fail(self, result: RunResult):
    #     # relation_name = getattr(result.node, 'relation_name', None)
    #     # if relation_name:
    #     #     relation_query = f"```select * from {relation_name.replace('`','')}```"
    #     relation = extract_table_info_from_compiled_code(getattr(result.node, 'compiled_code', ""))

    #     if relation:
    #         relation_query = f"\n   **Tabela:** `{relation['full_name']}`  \n"
    #         column_name = getattr(result.node, 'column_name', None)

    #         if column_name:
    #             relation_query += f"   **Coluna:** `{column_name}`  \n"
    #     else:
    #         relation_query = f"\n   **Tabela:** `Tabela não disponível`  \n"

    #     description = getattr(result.node, 'meta', {}).get('description')
    #     if description:
    #         relation_query += f"   **Descrição do teste:** `{description}`  \n"

    #     return f"`{result.node.name}`\n   {result.message}: {relation_query}"  # noqa

    # def warn(self, result: RunResult):
    #     # relation_name = getattr(result.node, 'relation_name', None)
    #     # if relation_name:
    #     #     relation_query = f"```select * from {relation_name.replace('`','')}```"
    #     relation = extract_table_info_from_compiled_code(getattr(result.node, 'compiled_code', None))

    #     if relation:
    #         relation_query = f"\n   **Tabela:** `{relation['full_name']}`  \n"
    #         column_name = getattr(result.node, 'column_name', None)

    #         if column_name:
    #             relation_query += f"   **Coluna:** `{column_name}`  \n"
    #     else:
    #         relation_query = f"\n   **Tabela:** `Tabela não disponível`  \n"

    #     description = getattr(result.node, 'meta', {}).get('description')
    #     if description:
    #         relation_query += f"   **Descrição do teste:** `{description}`  \n"

    #     return f"`{result.node.name}`\n   {result.message}: {relation_query}"  # noqa

    def _get_test_info(self, result: RunResult) -> str:
        """Extracts common information about the test (table, column, description)"""
        relation = extract_table_info_from_compiled_code(getattr(result.node, "compiled_code", ""))

        info_parts = []

        if relation:
            info_parts.append(f"\n   **Tabela:** `{relation['full_name']}`  \n")
            column_name = getattr(result.node, "column_name", None)
            if column_name:
                info_parts.append(f"   **Coluna:** `{column_name}`  \n")
        else:
            info_parts.append(f"\n   **Tabela:** `Tabela não disponível`  \n")

        description = getattr(result.node, "meta", {}).get("description")
        if description:
            info_parts.append(f"   **Descrição:** `{description}`  \n")

        return "".join(info_parts)

    def fail(self, result: RunResult):
        return f"`{result.node.name}`\n   {result.message}: {self._get_test_info(result)}"

    def warn(self, result: RunResult):
        return f"`{result.node.name}`\n   {result.message}: {self._get_test_info(result)}"


class FreshnessResultSummarizer:
    """
    A class that summarizes the freshness result of a DBT node.

    Methods:
    - summarize(result): Summarizes the freshness result based on its status.
    - error(result): Returns the error message for a failed freshness result.
    - fail(result): Returns the name of the failed freshness result.
    - warn(result): Returns the warning message for a stale freshness result.
    """

    def summarize(self, result: SourceFreshnessResult):
        if result.status == "error":
            return self.error(result)
        elif result.status == "fail":
            return self.fail(result)
        elif result.status == "warn":
            return self.warn(result)

    def error(self, result: SourceFreshnessResult):
        freshness = result.node.freshness
        error_criteria = f">={freshness.error_after.count} {freshness.error_after.period}"
        return f"{result.node.relation_name.replace('`', '')}: ({error_criteria})"

    def fail(self, result: SourceFreshnessResult):
        return f"{result.node.relation_name.replace('`', '')}"

    def warn(self, result: SourceFreshnessResult):
        freshness = result.node.freshness
        warn_criteria = f">={freshness.warn_after.count} {freshness.warn_after.period}"
        return f"{result.node.relation_name.replace('`', '')}: ({warn_criteria})"


class Summarizer:
    """
    A class that provides summarization functionality for different result types.
    This class can be called with a result object and it will return a summarized version
        of the result.

    Attributes:
        None

    Methods:
        __call__: Returns a summarized version of the given result object.

    """

    def __call__(self, result):
        if isinstance(result, RunResult):
            return RunResultSummarizer().summarize(result)
        elif isinstance(result, SourceFreshnessResult):
            return FreshnessResultSummarizer().summarize(result)
        else:
            raise ValueError(f"Unknown result type: {type(result)}")


# if __name__ == "__main__":
#     from pathlib import Path
#     target_path = Path(__file__).parents[2] / "tmp" / "dbt_log.txt"
#     print(target_path.as_posix())
