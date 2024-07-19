# -*- coding: utf-8 -*-
"""
This module defines a Prefect workflow for extracting and transforming data 
related to 'Disque Denúncia' reports.
"""

from pathlib import Path
from prefect import Flow
from pipelines.reports_disque_denuncia.extract.tasks import (
    get_reports_from_start_date,
    loop_transform_report_data
)


# Define the start date for retrieving reports
START_DATE = '2024-07-01'

# Define the path where raw XML files will be saved
file_path = Path(Path.cwd()) / 'pipelines' / 'reports_disque_denuncia' / 'data' / 'raw'

# Define the path where transformed CSV files will be saved
FINAL_PATH = 'pipelines/reports_disque_denuncia/data/partition_directory'

# Define the Prefect Flow for data extraction and transformation
with Flow(name='DataLake - Extração - Disque Denúncia') as extracao_disque_denuncia:

    # Task to get reports from the specified start date
    reports_response = get_reports_from_start_date(START_DATE, file_path, 'interesse')

    # Extract the list of XML file paths from the reports response
    source_path_list = reports_response['xml_file_path_list']

    # Task to transform the XML files into CSV files
    csv_path_list = loop_transform_report_data(source_path_list, FINAL_PATH)

### RUN ###
# Execute the flow and store the state of the execution
state = extracao_disque_denuncia.run()
