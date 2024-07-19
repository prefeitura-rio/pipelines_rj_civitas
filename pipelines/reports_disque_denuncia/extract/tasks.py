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
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import logging
import requests
import xmltodict
from pytz import timezone



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
