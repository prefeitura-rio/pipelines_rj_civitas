# -*- coding: utf-8 -*-
from typing import List, Literal

import pandas as pd
import requests
from google.auth import default
from googleapiclient.discovery import build
from prefeitura_rio.pipelines_utils.logging import log


def get_data_from_google_sheets(spreadsheet_id: str, range_name: str) -> List[List]:
    """
    Retrieves data from a specified range in a Google Sheets spreadsheet.

    This function uses Google Sheets API to access and read data from a
    specified range within a given spreadsheet. It requires valid
    authentication credentials obtained from the environment.

    Args:
        spreadsheet_id (str): The ID of the spreadsheet to retrieve data from.
        range_name (str): The A1 notation of the range to retrieve data from.

    Returns:
        List[List]: A list of lists containing the values retrieved from the
        specified range in the spreadsheet. Returns an empty list if no data
        is found.
    """
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    # Obter as credenciais da conta de serviço configurada no ambiente
    credentials, project = default(scopes=SCOPES)
    service = build("sheets", "v4", credentials=credentials)

    # Chamada à API para obter os dados
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )

    # Extrair os dados
    values = result.get("values", [])

    # Exibir os dados
    if not values:
        log("Nenhum dado encontrado.")

    return values


def send_data_to_google_sheets(
    spreadsheet_id: str,
    range_name: str,
    values: List[List[str]],
    insert_data_option: Literal["INSERT_ROWS", "OVERWRITE"] = "INSERT_ROWS",
    value_input_option: Literal["USER_ENTERED", "RAW"] = "USER_ENTERED",
):
    """
    Sends data to a specified Google Sheets spreadsheet.

    Parameters
    ----------
    spreadsheet_id : str
        The ID of the Google Sheets spreadsheet.
    range_name : str
        The range of cells where the data will be added, e.g., 'Sheet1!A1'.
    values : List[List[str]]
        The data to be added to the spreadsheet.
    insert_data_option : Literal["INSERT_ROWS", "OVERWRITE"], optional
        The option for inserting data. Default is "INSERT_ROWS".
    value_input_option : Literal["USER_ENTERED", "RAW"], optional
        The input option for data entry. Default is "USER_ENTERED".

    Returns
    -------
    None
        Prints the number of updated cells.
    """
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    # Obter as credenciais da conta de serviço configurada no ambiente
    credentials, project = default(scopes=SCOPES)
    service = build("sheets", "v4", credentials=credentials)

    body = {"values": values}

    # Usando a API para appendar os dados
    result = (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            insertDataOption=insert_data_option,
            valueInputOption=value_input_option,
            body=body,
        )
        .execute()
    )

    log(f"{range_name} - {result.get('updates').get('updatedCells')} células atualizadas.")


def api_civitas_get_token(api_url: str, user: str, password: str) -> str:
    url = api_url + "/auth/token"

    payload = {"grant_type": "password", "username": user, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.request("POST", url, data=payload, headers=headers)
        response.raise_for_status()
        token = response.json().get("access_token")
    except requests.exceptions.RequestException as e:
        log(f"Failed to retrieve token from {url}. Error: {str(e)}")
        return None

    return token


def get_waze_police_alerts(api_url: str, token: str) -> pd.DataFrame:

    if token is None:
        log("Token not found")
        return None

    url = api_url + "/waze/police"

    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.request("GET", url, headers=headers)
        response.raise_for_status()
        data: List[dict] = response.json()
    except requests.exceptions.RequestException as e:
        log(f"Error fetching data from Waze API: {e}")
        return None

    df = pd.DataFrame(data)
    return df
