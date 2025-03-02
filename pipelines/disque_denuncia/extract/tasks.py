# -*- coding: utf-8 -*-
"""
This module contains tasks for retrieving, processing, and saving XML reports.

Tasks include:
- Retrieving reports from a REST API
- Saving reports as XML files
- Parsing and normalizing XML data
- Transforming XML data into structured CSV files
"""

import glob
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

import basedosdados as bd
import googlemaps
import pandas as pd
import requests
import xmltodict
from google.cloud import bigquery
from prefect import task
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Skipped
from prefeitura_rio.pipelines_utils.bd import get_project_id
from prefeitura_rio.pipelines_utils.logging import log, log_mod
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from pytz import timezone

tz = timezone("America/Sao_Paulo")


def get_reports(
    start_date: str, tipo_difusao: str = "interesse", mod: int = 100, iter_counter: int = 0
) -> Dict[int, bytes]:
    """
    Retrieves reports from a specified start date and saves them as an XML file.

    Args:
        start_date (str): Start date for retrieving reports in ISO format
            (e.g., '2024-01-01').
        tipo_difusao (str): Type of diffusion expected. 'interesse' for specific
            or 'geral' for all subjects. Default is 'interesse'.
        mod (int): Only logs a message if the index is a multiple of mod. Default is 100.
        iter_counter (int): Actual index for log usage.

    Returns:
        Dict[int, bytes]: A dictionary with the quantity of reports and the XML
            bytes.

    Raises:
        ValueError: If the start_date format is incorrect.
        AttributeError: If tipo_difusao is neither 'geral' nor 'interesse'.

    Example:
        get_reports(start_date='2024-01-01')

    Note:
        Ensure the start date format adheres to ISO standards. This function
        saves the retrieved XML content as a file but does not return it
        directly.
    """
    log_mod(msg="Validating start_date format", level="info", index=iter_counter, mod=mod)
    try:
        # Ensure start_date is in 'yyyy-mm-dd' format
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError as exc:
        log(msg=f"Invalid start_date format: {exc}", level="error")
        raise ValueError("Incorrect date format, should be 'yyyy-mm-dd'") from exc

    log_mod(msg="Validating tipo_difusao", level="info", index=iter_counter, mod=mod)
    # Ensure that tipo_difusao is one of the allowed values
    if tipo_difusao.lower() not in (["geral", "interesse"]):
        log(msg=f"Invalid tipo_difusao: {tipo_difusao}", level="error")
        raise AttributeError(
            f"invalid tipo_difusao: {tipo_difusao}.\n" 'Must be "geral" or "interesse"'
        )

    url = f"https://proxy.civitas.rio/civitas/difusao_{tipo_difusao.lower()}/"
    params = {"fromdata": start_date}

    log_mod(msg="Sending request to API", level="info", index=iter_counter, mod=mod)
    response = requests.get(url, params=params, timeout=600)
    response.raise_for_status()

    log_mod(msg="Processing API response", level="info", index=iter_counter, mod=mod)
    # Get the response content and verify how many reports were returned
    xml_bytes = response.content
    report_qty = xmltodict.parse(response.text)["denuncias"]["@numTotal"]

    return {"report_qty": int(report_qty), "xml_bytes": xml_bytes}


def save_report_as_xml(
    file_dir: str | Path, xml_bytes: bytes, mod: int = 100, iter_counter: int = 0
) -> Dict[str, List[str]]:
    """
    Saves the XML bytes as a file and extracts report IDs.

    Args:
        file_dir (Path, str): Path for saving the file.
        xml_bytes (bytes): XML content to be saved.
        mod (int): Only logs a message if the index is a multiple of mod. Default is 100.
        iter_counter (int): Actual index for log usage.

    Returns:
        Dict[str, List[str]]: A dictionary containing the file path and a list of report IDs.
    """

    log_mod(msg="Saving XML file", level="info", index=iter_counter, mod=mod)
    root = ET.fromstring(xml_bytes)

    # Generating the file name
    xml_file_name = f"{datetime.now(tz=tz).strftime('%Y%m%d_%H%M%S_%f')}_report_disque_denuncia.xml"
    xml_file_path = file_dir / xml_file_name
    tree = ET.ElementTree(root)

    # Getting the reports ids and saving in a list with unique values
    report_id_list = list({element.get("id") for element in tree.findall("denuncia")})

    # Saving the xml file
    tree.write(str(xml_file_path), encoding="ISO-8859-1", xml_declaration=True)

    log_mod(msg="XML file saved", level="info", index=iter_counter, mod=mod)
    return {"xml_file_path": str(xml_file_path), "report_id_list": report_id_list}


def capture_reports(
    ids_list: List[str],
    start_date: str,
    tipo_difusao: str = "interesse",
    mod: int = 100,
    iter_counter: int = 0,
) -> List[Dict[str, str]]:
    """
    Capture reports using the provided IDs from the API endpoint.

    Args:
        ids_list (List[str]): List of report IDs to capture.
        start_date (str): Start date for retrieving reports.
        tipo_difusao (str): Type of diffusion expected. Default is 'interesse'.
        mod (int): Only logs a message if the index is a multiple of mod. Default is 100.
        iter_counter (int): Actual index for log usage.

    Returns:
        List[Dict[str, str]]: List of dictionaries with IDs and their response status.

    Raises:
        requests.HTTPError: If the API request fails with an HTTP error code.

    """
    log_mod(msg="Capturing reports from API", level="info", index=iter_counter, mod=mod)
    # Transforms the list into a string concatenated by |
    str_ids = "|".join(ids_list)

    # Construct the URL with the provided IDs

    url = f"https://proxy.civitas.rio/civitas/capturadas_{tipo_difusao}/"
    params = {"id": str_ids, "fromdata": start_date}

    try:
        # Make the GET request to capture the reports
        response_report = requests.get(url, params=params, timeout=600)
        response_report.raise_for_status()  # Raises an error if the response is unsuccessful

        log_mod(msg="Processing captured reports", level="info", index=iter_counter, mod=mod)
        # Returns a List of Dict with the ids and their response status
        ids_response = [element.attrib for element in ET.fromstring(response_report.content)]
        return ids_response

    except requests.exceptions.HTTPError as err:
        # Capture and re-raise the HTTP error for the caller
        log(msg=f"HTTP error occurred: {err}", level="error")
        raise requests.HTTPError(f"Request failed: {err}")


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_reports_from_start_date(
    start_date: str,
    file_dir: Path,
    tipo_difusao: str = "interesse",
    dataset_id: str = None,
    table_id: str = None,
    loop_limiter: bool = False,
    mod: int = 100,
) -> Dict[str, List[str]]:
    """
    Retrieves and processes reports from a start date until there are no more reports,
    saving them as XML files.

    Args:
        start_date (str): Start date for retrieving reports.
        file_dir (Path): Directory path for saving XML files.
        tipo_difusao (str): Type of diffusion expected. Default is 'interesse'.
        dataset_id (str): BigQuery data set id.
        table_id (str): BigQuery table_id.
        loop_limiter (int): Limits the loop iterations to 5 iterations.
            default is None, indicating that the loop will continue until the last date with data.
        mod (int): Only logs a message if the index is a multiple of mod. Default is 100.

    Returns:
        Dict[str, List[str]]: A dictionary containing a list of XML file paths and capture
            status lists.
    """
    log(msg="Creating directories if not exist", level="info")
    current_date = datetime.now(tz=tz).date()
    partition = (
        f"ano_particao={current_date.strftime('%Y')}/"
        f"mes_particao={current_date.strftime('%m')}/"
        f"data_particao={current_date}"
    )

    file_dir = Path(file_dir) / partition
    file_dir.mkdir(parents=True, exist_ok=True)

    last_page = False
    xml_file_path_list = []
    capture_status_list = []
    flow_run_mode = get_flow_run_mode()
    log(msg=f"Testing Run Mode: {flow_run_mode}", level="info")
    project_id = get_project_id(mode=flow_run_mode)
    storage_obj = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    iter_counter = 0

    log(msg="Starting report retrieval loop", level="info")
    while not last_page:

        log_mod(msg="Capturing reports from API", level="info", index=iter_counter, mod=mod)
        report_response = get_reports(
            start_date=start_date, tipo_difusao=tipo_difusao, mod=mod, iter_counter=iter_counter
        )
        log_mod(msg="Reports captured from API", level="info", index=iter_counter, mod=mod)

        if report_response["report_qty"] > 0:
            saved_xml = save_report_as_xml(
                file_dir=file_dir,
                xml_bytes=report_response["xml_bytes"],
                mod=mod,
                iter_counter=iter_counter,
            )
            log_mod(
                msg=f"Saving data to RAW: https://console.cloud.google.com/storage/browser/"
                f"{project_id}/raw/{dataset_id}/{table_id}",
                level="info",
                index=iter_counter,
                mod=mod,
            )
            storage_obj.upload(
                path=saved_xml["xml_file_path"],
                mode="raw",
                partitions=partition,
                if_exists="replace",
            )

            log_mod("XML files saved to RAW", level="info", index=iter_counter, mod=mod)
            xml_file_path_list.append(saved_xml["xml_file_path"])
            report_id_list = saved_xml["report_id_list"]

            if loop_limiter:
                break  # TEMPORARY LIMITER

            # Confirm that the data has been saved. The next iteration will display new 15 reports
            capture_status_list.extend(
                capture_reports(
                    ids_list=report_id_list,
                    start_date=start_date,
                    tipo_difusao=tipo_difusao,
                    mod=mod,
                    iter_counter=iter_counter,
                )
            )

        last_page = report_response["report_qty"] < 15

        iter_counter += 1

    return {"xml_file_path_list": xml_file_path_list, "capture_status_list": capture_status_list}


def parse_denuncia(denuncia: ET.Element) -> Dict[str, Union[str, List[Dict[str, str]]]]:
    """
    Parses a single 'denuncia' element into a dictionary.

    Args:
        denuncia (ET.Element): The 'denuncia' XML element.

    Returns:
        Dict[str, Union[str, List[Dict[str, str]]]]: A dictionary with parsed 'denuncia' data.
    """
    denuncia_dict = {
        "denuncia_numero": denuncia.get("numero", ""),
        "denuncia_id": denuncia.get("id", ""),
        "denuncia_parent_numero": denuncia.get("parentNumero", ""),
        "denuncia_parent_id": denuncia.get("parentId", ""),
        "data_denuncia": denuncia.get("dataDenuncia", ""),
        "data_difusao": denuncia.get("dataDifusao", ""),
        "denuncia_imediata": denuncia.get("imediata", ""),
        "orgaos": parse_orgaos(denuncia.find("orgaos")),
        "xptos": parse_xptos(denuncia.find("xptos")),
        "assuntos": parse_assuntos(denuncia.find("assuntos")),
        "endereco": parse_endereco(denuncia.find("endereco")).get("endereco"),
        "tipo_logradouro": parse_endereco(denuncia.find("endereco")).get("tipo_logradouro"),
        "descricao_logradouro": parse_endereco(denuncia.find("endereco")).get(
            "descricao_logradouro"
        ),
        "numero_logradouro": parse_endereco(denuncia.find("endereco")).get("numero_logradouro"),
        "complemento_logradouro": parse_endereco(denuncia.find("endereco")).get(
            "complemento_logradouro"
        ),
        "bairro": parse_endereco(denuncia.find("endereco")).get("bairro"),
        "subbairro": parse_endereco(denuncia.find("endereco")).get("subbairro"),
        "cep_logradouro": parse_endereco(denuncia.find("endereco")).get("cep_logradouro"),
        "referencia_logradouro": parse_endereco(denuncia.find("endereco")).get(
            "referencia_logradouro"
        ),
        "municipio": parse_endereco(denuncia.find("endereco")).get("municipio"),
        "estado": parse_endereco(denuncia.find("endereco")).get("estado"),
        "latitude": parse_gps(denuncia.find("gps")).get("latitude"),
        "longitude": parse_gps(denuncia.find("gps")).get("longitude"),
        "envolvidos": parse_envolvidos_dados(denuncia.find("envolvidos")),
        "relato": parse_relato(denuncia.find("relato")),
        "denuncia_status": parse_resultados(denuncia.find("resultados")),
    }
    return denuncia_dict


def parse_orgaos(orgaos: Optional[ET.Element]) -> List[Dict[str, str]]:
    """
    Parses 'orgaos' element into a list of dictionaries.

    Args:
        orgaos (Optional[ET.Element]): The 'orgaos' XML element.

    Returns:
        List[Dict[str, str]]: A list of dictionaries with parsed 'orgaos' data.
    """
    # Ensures that the columns will be created, even if the elements are missing in the XML
    if orgaos is None or not list(orgaos):
        return [{"orgao_id": "", "orgao_nome": "", "orgao_tipo": ""}]

    return [
        {
            "orgao_id": orgao.get("id", ""),
            "orgao_nome": orgao.get("nome", ""),
            "orgao_tipo": orgao.get("tipo", ""),
        }
        for orgao in orgaos.findall("orgao")
    ]


def parse_xptos(xptos: Optional[ET.Element]) -> List[Dict[str, str]]:
    """
    Parses 'xptos' element into a list of dictionaries.

    Args:
        xptos (Optional[ET.Element]): The 'xptos' XML element.

    Returns:
        List[Dict[str, str]]: A list of dictionaries with parsed 'xptos' data.
    """
    # Ensures that the columns will be created, even if the elements are missing in the XML
    if xptos is None or not list(xptos):
        return [{"xpto_id": "", "xpto_nome": ""}]

    return [
        {"xpto_id": xpto.get("id", ""), "xpto_nome": xpto.get("nome", "")}
        for xpto in xptos.findall("xpto")
    ]


def parse_assuntos(assuntos: Optional[ET.Element]) -> List[Dict[str, str]]:
    """
    Parses 'assuntos' element into a list of dictionaries.

    Args:
        assuntos (Optional[ET.Element]): The 'assuntos' XML element.

    Returns:
        List[Dict[str, str]]: A list of dictionaries with parsed 'assuntos' data.
    """
    # Ensures that the columns will be created, even if the elements are missing in the XML
    if assuntos is None or not list(assuntos):
        return [
            {
                "assunto_classe_id": "",
                "assunto_classe": "",
                "assunto_tipo_id": "",
                "assunto_tipo": "",
                "assunto_principal": "",
            }
        ]

    return [
        {
            "assunto_classe_id": assunto.get("cla_cd"),
            "assunto_classe": (classe.text.strip() if classe is not None else ""),
            "assunto_tipo_id": assunto.get("tpa_cd"),
            "assunto_tipo": (tipo.text.strip() if tipo is not None else ""),
            "assunto_principal": assunto.get("ass_principal"),
        }
        for assunto in assuntos.findall("assunto")
        for classe in [assunto.find("classe")]
        for tipo in [assunto.find("tipo")]
    ]


def parse_endereco(endereco: Optional[ET.Element]) -> Dict[str, str]:
    """
    Parses 'endereco' element into a dictionary.

    Args:
        endereco (Optional[ET.Element]): The 'endereco' XML element.

    Returns:
        Dict[str, str]: A dictionary with parsed 'endereco' data.
    """
    # Ensures that the columns will be created, even if the elements are missing in the XML
    if endereco is None:
        return {
            "endereco": "",
            "tipo_logradouro": "",
            "descricao_logradouro": "",
            "numero_logradouro": "",
            "complemento_logradouro": "",
            "bairro": "",
            "subbairro": "",
            "cep_logradouro": "",
            "referencia_logradouro": "",
            "municipio": "",
            "estado": "",
        }

    def get_text(element: ET.Element, tag: str) -> str:
        """Helper function to extract text from XML element."""
        sub_element = element.find(tag)
        return sub_element.text.strip() if sub_element.text is not None else ""

    return {
        "endereco": get_text(endereco, "endereco"),
        "tipo_logradouro": get_text(endereco, "den_logr_tp"),
        "descricao_logradouro": get_text(endereco, "den_logr_ds"),
        "numero_logradouro": get_text(endereco, "den_logr_num"),
        "complemento_logradouro": get_text(endereco, "den_logr_cmpl"),
        "bairro": get_text(endereco, "bairro"),
        "subbairro": get_text(endereco, "den_logr_subbairro"),
        "cep_logradouro": get_text(endereco, "den_logr_cep"),
        "referencia_logradouro": get_text(endereco, "den_loc_ref"),
        "municipio": get_text(endereco, "municipio"),
        "estado": get_text(endereco, "estado"),
    }


def parse_gps(gps: Optional[ET.Element]) -> Dict[str, str]:
    """
    Parses 'gps' element into a dictionary.

    Args:
        gps (Optional[ET.Element]): The 'gps' XML element.

    Returns:
        Dict[str, str]: A dictionary with parsed 'gps' data.
    """
    # Ensures that the columns will be created, even if the elements are missing in the XML
    if gps is None:
        return {"latitude": "", "longitude": ""}

    return {
        "latitude": gps.find("lat").text.strip() if gps.find("lat").text is not None else "",
        "longitude": gps.find("long").text.strip() if gps.find("long").text is not None else "",
    }


def parse_resultados(resultados: Optional[ET.Element]) -> List[Dict[str, str]]:
    """
    Parses 'resultados' element into a string.

    Args:
        resultados (Optional[ET.Element]): The 'resultados' XML element.

    Returns:
        str: The parsed 'resultados' text.
    """
    # Ensures that the columns will be created, even if the elements are missing in the XML
    if resultados is None or not list(resultados):
        return [{"denuncia_status": ""}]

    return [{"denuncia_status": resultado.text} for resultado in resultados.findall("status")]


def parse_envolvidos_dados(envolvidos: Optional[ET.Element]) -> List[Dict[str, str]]:
    """
    Parses 'envolvidos' element into a list of dictionaries.

    Args:
        envolvidos (Optional[ET.Element]): The 'envolvidos' XML element.

    Returns:
        List[Dict[str, str]]: A list of dictionaries with parsed 'envolvidos' data.
    """
    # Ensures that the columns will be created, even if the elements are missing in the XML
    if envolvidos is None or not list(envolvidos):
        return [
            {
                "envolvido_id": "",
                "envolvido_nome": "",
                "envolvido_vulgo": "",
                "envolvido_sexo": "",
                "envolvido_idade": "",
                "envolvido_pele": "",
                "envolvido_estatura": "",
                "envolvido_porte": "",
                "envolvido_cabelos": "",
                "envolvido_olhos": "",
                "envolvido_outras_caracteristicas": "",
            }
        ]

    return [
        {
            "envolvido_id": envolvido.get("env_cd"),
            "envolvido_nome": dado.find("nome").text.strip(),
            "envolvido_vulgo": dado.find("vulgo").text.strip(),
            "envolvido_sexo": dado.find("sexo").text.strip(),
            "envolvido_idade": dado.find("idade").text.strip(),
            "envolvido_pele": caracteristica.find("pele").text.strip(),
            "envolvido_estatura": caracteristica.find("estatura").text.strip(),
            "envolvido_porte": caracteristica.find("porte").text.strip(),
            "envolvido_cabelos": caracteristica.find("cabelos").text.strip(),
            "envolvido_olhos": caracteristica.find("olhos").text.strip(),
            "envolvido_outras_caracteristicas": caracteristica.find("outras").text.strip(),
        }
        for envolvido in envolvidos.findall("envolvido")
        for dado in envolvido.findall("dados")
        for caracteristica in envolvido.findall("caracteristicas")
    ]


def parse_relato(relato: Optional[ET.Element]) -> List[Dict[str, str]]:
    """
    Parses 'relato' element into a string.

    Args:
        relato (Optional[ET.Element]): The 'relato' XML element.

    Returns:
        str: The parsed 'relato' text.
    """
    # Ensures that the columns will be created, even if the elements are missing in the XML
    if relato is None:
        return ""

    return relato.text


def explode_and_normalize(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Explodes a column in a DataFrame and normalizes its nested structure.

    Args:
        df (pd.DataFrame): The DataFrame containing the column to explode.
        column (str): The name of the column to explode.

    Returns:
        pd.DataFrame: The DataFrame with the exploded and normalized column.

    """
    df_exploded = df.explode(column, ignore_index=True)
    df_normalized = pd.json_normalize(df_exploded[column])
    return pd.concat([df_exploded.drop(columns=[column]), df_normalized], axis=1)


def process_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes date columns in the given DataFrame

    - Converts 'data_hora_difusao' to datetime
    - Creates a new column 'data_hora_difusao' with the format 'YYYYMMDD_HH'
    - Converts 'data_denuncia' to datetime with the format '%Y-%m-%d %H:%M:%S.

    Parameters:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    df["datetime_difusao"] = pd.to_datetime(df["data_difusao"], format="%d/%m/%Y %H:%M")
    df["data_difusao"] = df["datetime_difusao"].dt.date
    df["hora_difusao"] = df["datetime_difusao"].dt.time

    df["datetime_denuncia"] = pd.to_datetime(df["data_denuncia"], format="%d/%m/%Y %H:%M")
    df["data_denuncia"] = df["datetime_denuncia"].dt.date
    df["hora_denuncia"] = df["datetime_denuncia"].dt.time

    return df


def transform_report_data(
    source_file_path: str, final_file_dir: str, mod: int = 100, iter_counter: int = 0
) -> List[str]:
    """
    Transforms XML report data into a structured CSV and extracts report IDs.

    This function reads an XML file containing report data, processes it into a pandas DataFrame,
    normalizes nested structures, and saves the final DataFrame as a CSV file. The function also
    extracts unique report IDs from the data.

    Args:
        source_file_path (str): The file path of the source XML file.
        final_file_dir (str): The directory path where the CSV file will be saved.
        mod (int): Only logs a message if the index is a multiple of mod. Default is 100.
        iter_counter (int): Actual index for log usage.

    Returns:
        list: A list of unique report IDs extracted from the data.

    Example:
        source_file_path = '/path/to/source_report.xml'
        final_file_dir = '/path/to/save_directory'
        report_ids = transform_report_data(source_file_path, final_file_dir)
        print(report_ids)  # Outputs a list of unique report IDs
    """
    log_mod(msg="Transforming XML files into CSV", level="info", index=iter_counter, mod=mod)

    def get_formatted_file_dir(date: datetime, hour: datetime) -> Path:
        """Helper function to format the file path based on the date and hour."""
        return (
            Path(final_file_dir)
            / f"ano_particao={date.strftime('%Y')}"
            / f"mes_particao={date.strftime('%m')}"
            / f"data_particao={date}"
        )

    log_mod(msg="Reading XML files", level="info", index=iter_counter, mod=mod)
    with open(source_file_path, "r", encoding="ISO-8859-1") as file:
        # xml_bytes = file.read()
        try:
            root = ET.fromstring(file.read())
        except ET.ParseError as e:
            log(msg=f"Failed to parse XML {e}", level="error")
            raise

    log_mod(msg="Reading XML elements", level="info", index=iter_counter, mod=mod)
    denuncias_list = [parse_denuncia(denuncia) for denuncia in root.findall("denuncia")]

    log_mod(msg="Creating DataFrame from parsed data", level="info", index=iter_counter, mod=mod)
    df = pd.DataFrame(denuncias_list)

    log_mod(
        msg="Exploding, normalizing columns and removing duplicated rows",
        level="info",
        index=iter_counter,
        mod=mod,
    )
    for col in ["xptos", "orgaos", "assuntos", "envolvidos", "denuncia_status"]:
        df = explode_and_normalize(df, col)

    df = process_datetime_columns(df)
    df = df.drop_duplicates()

    # Partition by
    changed_file_path_list = []
    for (data_denuncia, hora_denuncia), group in df.groupby(["data_denuncia", "hora_denuncia"]):
        file_dir = get_formatted_file_dir(data_denuncia, hora_denuncia)

        # Ensure the final directory exists
        file_dir.mkdir(parents=True, exist_ok=True)

        # Compose the file_path with the current datetime
        file_path = (
            file_dir
            / f"{datetime.now(tz=tz).strftime('%Y%m%d_%H%M%S_%f')}_report_disque_denuncia.csv"
        )

        # Set header to False if file already exists
        header_option = not file_path.exists()

        group.to_csv(file_path, header=header_option, mode="a", index=False)

        changed_file_path_list.append(str(file_path))
    log_mod(msg=f"Files saved in {file_dir}", level="info", index=iter_counter, mod=mod)

    return list(set(changed_file_path_list))


@task
def loop_transform_report_data(
    source_file_path_list: List[str], final_file_dir: str | Path, mod: int = 100
) -> List[str]:
    """
    Processes multiple XML report files into structured CSVs and extracts report IDs.

    This function iterates over a list of XML file paths, transforms each file into a structured
    CSV using the `transform_report_data` function, and collects the file paths of the saved CSVs.
    It ensures that each file path is unique in the final list of changed file paths.

    Args:
        source_file_path_list (List[str]): A list of file paths for the source XML files.
        final_file_dir (str): The directory path where the CSV files will be saved.
        mod (int): Only logs a message if the index is a multiple of mod. Default is 100.

    Returns:
        List[str]: A list of unique file paths for the CSV files that were saved.

    Example:
        source_file_path_list = ['/path/to/source_report1.xml', '/path/to/source_report2.xml']
        final_file_dir = '/path/to/save_directory'
        changed_file_paths = loop_transform_report_data(source_file_path_list, final_file_dir)
        print(changed_file_paths)  # Outputs a list of unique file paths for the saved CSVs
    """
    changed_file_path_list = []

    final_file_dir = Path(final_file_dir)
    final_file_dir.mkdir(parents=True, exist_ok=True)
    iter_counter = 0

    for file_path in source_file_path_list:
        changed_file_path_list.extend(
            transform_report_data(
                source_file_path=file_path,
                final_file_dir=final_file_dir,
                mod=mod,
                iter_counter=iter_counter,
            )
        )
        iter_counter += 1

    saved_csv_path_str = "\n".join(
        [
            file
            for file in glob.glob((final_file_dir / "**").as_posix(), recursive=True)
            if file.endswith(".csv")
        ]
    )
    log(f"CSV files saved: {saved_csv_path_str}")

    return list(set(changed_file_path_list))


# Check if there are any reports returned
@task
def check_report_qty(reports_response):
    if not reports_response["xml_file_path_list"]:
        log("No data returned by the API, finishing the flow.", level="info")
        skip = Skipped(message="No data returned by the API, finishing the flow.")
        raise ENDRUN(state=skip)


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def update_missing_coordinates_in_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    id_column_name: str,
    address_columns_names: list[str],
    lat_lon_columns_names: dict[str, str],
    api_key: str,
    region: str = "br",
    mode: Literal["prod", "staging"] = "staging",
    date_execution: str = None,
    start_date: str = None,
    date_column_name: str = None,
    timestamp_creation_column_name: str = None,
    timezone_str: str = "America/Sao_Paulo",
) -> None:
    """Updates missing coordinates in BigQuery using Google Maps Geocoding API.

    Updates latitude and longitude coordinates for addresses in a BigQuery table by geocoding
    addresses using the Google Maps API. Only processes rows where coordinates are missing.

    Args:
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        id_column_name (str): Name of the ID column in the table.
        address_columns_names (list[str]): List of column names containing address parts.
        lat_lon_columns_names (dict[str, str]): Dictionary mapping 'latitude' and 'longitude'
            to their respective column names.
        api_key (str): Google Maps API key for geocoding.
        region (str, optional): Region code for geocoding. Defaults to "br".
        mode (Literal["prod", "staging"], optional): Execution mode. Defaults to "staging".
        start_date (str, optional): Filter records from this date. Defaults to None.
        date_column_name (str, optional): Name of date column for filtering. Defaults to None.
        timestamp_creation_column_name (str, optional): Name of timestamp creation column.
            Defaults to None.
        timezone_str (str, optional): Timezone string. Defaults to "America/Sao_Paulo".

    Returns:
        int: Number of rows successfully updated, or 0 if no updates needed.

    Raises:
        Exception: If geocoding fails or BigQuery update operation fails.
    """
    # Adjust dataset for staging mode
    dataset_id += "_staging" if mode == "staging" else ""

    # Initialize BigQuery and Google Maps clients
    bd.config.billing_project_id = project_id
    bd.config.from_file = True
    bq_client = bigquery.Client()
    client = googlemaps.Client(key=api_key)

    # Query rows with missing latitude or longitude
    address_columns_str = "`, `".join(address_columns_names)
    query = f"""
    SELECT
        `{id_column_name}`,
        `{address_columns_str}`
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE
        (`{lat_lon_columns_names['latitude']}` IS NULL OR `{lat_lon_columns_names['longitude']}` IS NULL)
    """

    if start_date:
        query += f" AND `{date_column_name}` >= '{start_date}'"

    elif date_execution:
        query += f" AND `{timestamp_creation_column_name}` >= '{date_execution}'"

    else:
        raise ValueError("start_date or date_execution is required.")

    log(f"Running query: {query}")
    data = bd.read_sql(query)

    if len(data) == 0:
        log("No rows found with missing latitude/longitude.")
        return 0
    else:
        log(f"Found {len(data)} rows with missing latitude/longitude.")

    def filter_address_parts(address_parts: list[str]) -> list[str]:
        final_address = []
        dispensable_parts = [
            "não informado",
            "nao informado",
            "ni",
            "n.i",
            "n.i.",
            "ni.",
            "",
            "0",
            "nan",
        ]

        for part in address_parts:
            if str(part).lower() not in dispensable_parts:
                final_address.append(str(part))
        return final_address

    # Geocode rows and prepare updates
    updates = []
    errors = []

    for i, row in data.iterrows():
        address_parts = [row.get(col) for col in address_columns_names]
        filtered_parts = filter_address_parts(address_parts)
        full_address = ", ".join(filtered_parts)

        try:
            log_mod(f"Geocoding address - {i}/{len(data)}", index=i, mod=10)
            geocode_result = client.geocode(full_address, region=region)

            if geocode_result:
                location = geocode_result[0]["geometry"]["location"]

                new_data = {
                    f"{id_column_name}": row.get(id_column_name),
                    f"{lat_lon_columns_names['latitude']}": location["lat"],
                    f"{lat_lon_columns_names['longitude']}": location["lng"],
                }

                if timestamp_creation_column_name:
                    new_data.update(
                        {
                            f"{timestamp_creation_column_name}": datetime.now(
                                tz=timezone(timezone_str)
                            ).strftime("%Y-%m-%d %H:%M:%S.%f")
                        }
                    )

                updates.append(new_data)
        except Exception as e:
            log(f"Error geocoding address '{full_address}': {e}")
            errors.append(full_address)

    # If there are updates, prepare and execute the MERGE query
    if updates:
        log(
            f"Updating latitude and longitude for {len(updates)} rows in {project_id}.{dataset_id}.{table_id}"
        )

        # Criar a lista de STRUCTs
        struct_list = []
        for update in updates:
            struct_list.append(
                f"""STRUCT(
                    '{update['id_denuncia']}' as id_denuncia,
                    {update['latitude']} as latitude,
                    {update['longitude']} as longitude,
                    DATETIME('{update['timestamp_insercao']}') as timestamp_insercao
                )"""
            )

        structs = ",\n".join(struct_list)

        # Prepare the MERGE query
        merge_query = f"""
        MERGE `{project_id}.{dataset_id}.{table_id}` AS target
        USING (
            SELECT * FROM UNNEST([
                {structs}
            ])
        ) AS source
        ON target.id_denuncia = source.id_denuncia
        WHEN MATCHED THEN
            UPDATE SET
                target.latitude = source.latitude,
                target.longitude = source.longitude,
                target.timestamp_insercao = source.timestamp_insercao
        """

        # Execute the MERGE query
        try:
            bq_client.query(merge_query).result()
            log(f"Successfully updated latitude and longitude for {len(updates)} rows.")
            updated_rows = len(updates)

        except Exception as e:
            log(f"Error updating latitude and longitude for {len(updates)} rows: {e}")
            updated_rows = 0

    return updated_rows


@task
def task_get_date_execution(utc: bool = False) -> str:
    if utc:
        date_execution = datetime.now(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    else:
        date_execution = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")

    return date_execution
