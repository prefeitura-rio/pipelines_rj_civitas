# -*- coding: utf-8 -*-
"""
This module contains tasks for retrieving, processing, and saving XML reports.

Tasks include:
- Retrieving reports from a REST API
- Saving reports as XML files
- Parsing and normalizing XML data
- Transforming XML data into structured CSV files
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Union, Optional
import logging
import requests
import xmltodict
from prefect import task
from pytz import timezone
import pandas as pd


tz = timezone("America/Sao_Paulo")
logging.basicConfig(level=logging.INFO)


def get_reports(start_date: str, tipo_difusao: str = "interesse") -> Dict[int, bytes]:
    """
    Retrieves reports from a specified start date and saves them as an XML file.

    Args:
        start_date (str): Start date for retrieving reports in ISO format 
            (e.g., '2024-01-01').
            
        tipo_difusao (str): Type of diffusion expected. 'interesse' for specific 
            or 'geral' for all subjects. Default is 'interesse'.

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
    try:
        # Ensure start_date is in 'yyyy-mm-dd' format
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Incorrect date format, should be 'yyyy-mm-dd'") from exc

    # Ensure that tipo_difusao is one of the allowed values
    if tipo_difusao.lower() not in (["geral", "interesse"]):
        raise AttributeError(
            f"invalid tipo_difusao: {tipo_difusao}.\n" 'Must be "geral" or "interesse"'
        )

    url = f"https://proxy.dados.rio:3380/civitas/difusao_{tipo_difusao.lower()}/"
    params = {"fromdata": start_date}

    response = requests.get(url, params=params, timeout=600)
    response.raise_for_status()

    # Get the response content and verify how many reports were returned
    xml_bytes = response.content
    report_qty = xmltodict.parse(response.text)["denuncias"]["@numTotal"]

    return {"report_qty": int(report_qty), "xml_bytes": xml_bytes}


def save_report_as_xml(file_path: str, xml_bytes: bytes) -> Dict[str, List[str]]:
    """
    Saves the XML bytes as a file and extracts report IDs.

    Args:
        file_path (str): Path for saving the file.
        xml_bytes (bytes): XML content to be saved.

    Returns:
        Dict[str, List[str]]: A dictionary containing the file path and a list of report IDs.
    """
    # Save the xml file if there is some data

    root = ET.fromstring(xml_bytes)

    # Generating the file name
    xml_file_name = f"{datetime.now(tz=tz).strftime('%Y%m%d_%H%M%S_%f')}_report_disque_denuncia.xml"
    xml_file_path = Path(file_path / xml_file_name)
    tree = ET.ElementTree(root)

    # Getting the reports ids and saving in a list with unique values
    report_id_list = list({element.get("id") for element in tree.findall("denuncia")})


    # Saving the xml file
    tree.write(xml_file_path, encoding="ISO-8859-1", xml_declaration=True)

    return {"xml_file_path": str(xml_file_path), "report_id_list": report_id_list}


def capture_reports(
    ids_list: List[str], start_date: str, tipo_difusao: str = "interesse"
    ) -> List[Dict[str, str]]:
    """
    Capture reports using the provided IDs from the API endpoint.

    Args:
        ids_list (List[str]): List of report IDs to capture.
        start_date (str): Start date for retrieving reports.
        tipo_difusao (str): Type of diffusion expected. Default is 'interesse'.

    Returns:
        List[Dict[str, str]]: List of dictionaries with IDs and their response status.

    Raises:
        requests.HTTPError: If the API request fails with an HTTP error code.

    """
    # Transforms the list into a string concatenated by |
    str_ids = "|".join(ids_list)

    # Construct the URL with the provided IDs

    url = f"https://proxy.dados.rio:3380/civitas/capturadas_{tipo_difusao}/"
    params = {"id": str_ids, "fromdata": start_date}

    try:
        # Make the GET request to capture the reports
        response_report = requests.get(url, params=params, timeout=600)
        response_report.raise_for_status()  # Raises an error if the response is unsuccessful

        # Returns a List of Dict with the ids and their response status
        ids_response = [element.attrib for element in ET.fromstring(response_report.content)]
        return ids_response

    except requests.exceptions.HTTPError as err:
        # Capture and re-raise the HTTP error for the caller
        raise requests.HTTPError(f"Request failed: {err}")


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_reports_from_start_date(
    start_date: str, file_path: Path, tipo_difusao: str = "interesse"
    ) -> Dict[str, List[str]]:
    """
    Retrieves and processes reports from a start date until there are no more reports, 
    saving them as XML files.

    Args:
        start_date (str): Start date for retrieving reports.
        file_path (Path): Directory path for saving XML files.
        tipo_difusao (str): Type of diffusion expected. Default is 'interesse'.

    Returns:
        Dict[str, List[str]]: A dictionary containing a list of XML file paths and capture 
            status lists.
    """

    temp_limiter = 0  # TEMPORARY LIMITER
    last_page = False
    xml_file_path_list = []
    capture_status_list = []

    while not last_page:
        report_response = get_reports(start_date=start_date, tipo_difusao=tipo_difusao)

        if report_response["report_qty"] > 0:
            saved_xml = save_report_as_xml(
                file_path=file_path, xml_bytes=report_response["xml_bytes"]
            )

            xml_file_path_list.append(saved_xml["xml_file_path"])

            # Confirm that the data has been saved. The next iteration will display new 15 reports
            report_id_list = saved_xml["report_id_list"]
            capture_status_list.extend(capture_reports(report_id_list, start_date, tipo_difusao))

        last_page = report_response["report_qty"] < 15

        temp_limiter += 1  # TEMPORARY LIMITER
        if temp_limiter >= 2:  # TEMPORARY LIMITER
            break  # TEMPORARY LIMITER

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
        "bairro": parse_endereco(denuncia.find("endereco")).get("bairro"),
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
        return [{"classe": "", "tipo": ""}]

    return [
        {
            "assunto_classe": (classe.text.strip() if classe is not None else ""),
            "assunto_tipo": (tipo.text.strip() if tipo is not None else ""),
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
        return {"endereco": "", "bairro": "", "municipio": "", "estado": ""}

    return {
        "endereco": (
            endereco.find("endereco").text.strip() if endereco.find("endereco") is not None else ""
        ),
        "bairro": (
            endereco.find("bairro").text.strip() if endereco.find("bairro") is not None else ""
        ),
        "municipio": (
            endereco.find("municipio").text.strip()
            if endereco.find("municipio") is not None
            else ""
        ),
        "estado": (
            endereco.find("estado").text.strip() if endereco.find("estado") is not None else ""
        ),
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


def transform_report_data(source_file_path: str, final_path: str) -> List[str]:
    """
    Transforms XML report data into a structured CSV and extracts report IDs.

    This function reads an XML file containing report data, processes it into a pandas DataFrame, 
    normalizes nested structures, and saves the final DataFrame as a CSV file. The function also 
    extracts unique report IDs from the data.

    Args:
        source_file_path (str): The file path of the source XML file.
        final_path (str): The directory path where the CSV file will be saved.

    Returns:
        list: A list of unique report IDs extracted from the data.

    Example:
        source_file_path = '/path/to/source_report.xml'
        final_path = '/path/to/save_directory'
        report_ids = transform_report_data(source_file_path, final_path)
        print(report_ids)  # Outputs a list of unique report IDs
    """

    def get_formatted_file_path(date: datetime, hour: datetime) -> Path:
        """Helper function to format the file path based on the date and hour."""
        return (Path(final_path) / f"ano_particao={date.strftime('%Y')}" /
                f"mes_particao={date.strftime('%m')}" / f"data_particao={date}" / 
                f"{date.strftime('%Y%m%d')}_{hour.strftime('%H')}.csv")

    logging.info("Reading XML file")
    with open(source_file_path, "r", encoding="ISO-8859-1") as file:
        # xml_bytes = file.read()
        try:
            root = ET.fromstring(file.read())
        except ET.ParseError as e:
            logging.error("Failed to parse XML %s", e)
            raise

    denuncias_list = [parse_denuncia(denuncia) for denuncia in root.findall("denuncia")]

    logging.info("Creating DataFrame from parsed data")
    df = pd.DataFrame(denuncias_list)

    logging.info("Exploding, normalizing columns and removing duplicated rows")
    for col in ["xptos", "orgaos", "assuntos", "envolvidos", "denuncia_status"]:
        df = explode_and_normalize(df, col)

    df = process_datetime_columns(df)
    df = df.drop_duplicates()

    # Partition by
    changed_file_path_list = []
    for (data_difusao, hora_difusao), group in df.groupby(["data_difusao", "hora_difusao"]):
        file_path = get_formatted_file_path(data_difusao, hora_difusao)

        # Ensure the final directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Set header to False if file already exists
        header_option = not file_path.exists()

        logging.info("Saving reports to %s", file_path)
        group.to_csv(file_path, header=header_option, mode="a", index=False)

        changed_file_path_list.append(str(file_path))

    return list(set(changed_file_path_list))
