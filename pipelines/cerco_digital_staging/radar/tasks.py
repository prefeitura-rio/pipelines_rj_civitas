# -*- coding: utf-8 -*-
# pylint: disable=E1101
"""
This module provides tasks for downloading data from CETRIO API and uploading it to BigQuery.
"""

import csv
from pathlib import Path
from uuid import uuid4
import pandas as pd
import requests
from prefect import task

from prefeitura_rio.pipelines_utils.logging import log
from slugify.slugify import QUOTE_PATTERN


@task
def extract_radar_data(
    secrets: dict,
    filename: str,
) -> Path:
    """
    Extracts radar data from CETRIO API and saves it to a local csv file.

    Args:
        secrets: Dictionary containing the secrets for the CETRIO API.
        filename: Name of the file to save the data to.

    Returns:
        Path: Path to the extracted radar data file.

    Raises:
        Exception: If there's an error extracting the radar data.
    """
    path = Path(f"/tmp/{uuid4()}")
    path.mkdir(parents=True, exist_ok=True)

    filepath = path / filename

    url = secrets.get('URL')
    token = secrets.get('TOKEN')
    
    log(f"Starting download from URL: {url}")

    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        return filepath
    except Exception as e:
        log(f"Error extracting radar data from CETRIO API: {str(e)}", "error")
        raise