# -*- coding: utf-8 -*-
"""
This module contains tasks for retrieving, processing, and saving XML reports.

Tasks include:
- Retrieving reports from a REST API
- Saving reports as XML files
- Parsing and normalizing XML data
- Transforming XML data into structured CSV files.
"""


from datetime import timedelta
import os
import prefeitura_rio.pipelines_utils.bd as bd
import pandas as pd
import requests
import urllib3
from prefeitura_rio.core import settings
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from pytz import timezone
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Skipped, Failed

tz = timezone("America/Sao_Paulo")
# Disable the warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def save_cameras_raw_file(cameras: list[dict]):
    if not cameras:
        msg = "cameras is empty"
        log(msg, level="info")
        fail = Failed(message=msg)
        raise ENDRUN(state=fail)
    
    df = pd.DataFrame(cameras)
    log(msg=f"Saving cameras raw file", level="info")
    df.to_csv(
        "cameras.csv", 
        sep=",", 
        quotechar='"', 
        quoting=1, # quote all fields
        encoding="utf-8",
        index=False)
    
    
@task(
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT, 
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT)
)
def get_cameras() -> list[dict]:
    url = os.getenv("API_TIXXI_BASE_URL")
    log(msg=f"Getting cameras", level="info")
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        data = response.json()
        if not data:
            msg = "No cameras found"
            log(msg, level="info")
            fail = Failed(message=msg)
            raise ENDRUN(state=fail)
        else:
            log(msg=f"{len(data)} cameras found")
            return data
            
    except requests.exceptions.HTTPError as err:
        log(msg=f"Request failed: {err}", level="error")
        raise requests.HTTPError(f"Request failed: {err}")
    

@task(
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
def create_table_and_upload_to_gcs(
    cameras: list[dict],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    biglake_table: bool = True,
    source_format: str = "csv",
) -> None:
    if not cameras:
        log(msg="No data to upload", level="info")
        return
    
    # Save temp file
    save_cameras_raw_file(cameras)
    
    bd.create_table_and_upload_to_gcs(
        data_path="cameras.csv",
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
        source_format=source_format,
    )
