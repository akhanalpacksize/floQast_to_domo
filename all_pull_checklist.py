import os
import logging
import requests
import datetime
import pandas as pd
import concurrent.futures
from logger_config import setup_logging
from config.env import flo_base, access_token
from commons import output_dir, Checklist_file
from json_to_csv import json_to_dataframe
from upload_csv_to_dataset_checklist import generate_update_schema, upload_csv

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

setup_logging(module_name="floqast_data_pull")

logger = logging.getLogger(__name__)

start_date = datetime.datetime(2018, 9,1)
end_date = datetime.datetime.now()

base_url = flo_base


def fetch_data_for_month(month, base_url, access_token):
    url = f'{base_url}/analytics/checklists?filter[month]={month.strftime("%B")}&filter[year]={month.year}'

    headers = {'x-api-key': access_token}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()['data']
        flattened_data_list = json_to_dataframe(data)
        df = pd.DataFrame(flattened_data_list)
        logger.info(f'Fetching data for {month.strftime("%B %Y")} for checklists')
        return df
    else:
        logger.error(f'Error fetching data for {month.strftime("%B %Y")}: {response.status_code}')
        return None


def fetch_all_Checklists_data_parallel():
    all_dfs = []

    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Use executor.map to asynchronously fetch data for multiple months
        dfs = executor.map(
            lambda month: fetch_data_for_month(month, base_url, access_token),
            pd.date_range(start=start_date, end=end_date, freq='MS')
        )

        # Collect the results and filter out None values
        all_dfs.extend([df for df in dfs if df is not None])

    folder_path = output_dir
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    final_df = pd.concat(all_dfs, ignore_index=True)

    csv_file_path = os.path.join(folder_path, Checklist_file)
    final_df.to_csv(csv_file_path, index=False)

    logger.info(f'CSV created in {csv_file_path}')


