import os
import logging
import requests
import datetime
import pandas as pd
import concurrent.futures
from logger_config import setup_logging
from json_to_csv_for_recon import json_to_dataframe
from dateutil.relativedelta import relativedelta

from config.env import flo_base, access_token
from commons import output_dir, Reconciliations
from upload_csv_to_dataset_reconciliation import generate_update_schema, upload_csv

# Setup logging
setup_logging(module_name="create_dataset")

logger = logging.getLogger(__name__)

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

start_date = datetime.datetime(2018, 9, 1)
end_date = datetime.datetime.now() - relativedelta(days=datetime.datetime.now().day)
base_url = flo_base


def fetch_data_for_month(month, base_url, access_token):
    url = f'{base_url}/reconciliations?filter[month]={month.strftime("%B")}&filter[year]={month.year}'
    headers = {'x-api-key': access_token}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()['data']
        flattened_data_list = json_to_dataframe(data)
        df = pd.DataFrame(flattened_data_list)
        df["Month"] = month.strftime("%B")
        df['Year'] = month.year
        df = df[['Year', 'Month'] + [col for col in df.columns if col not in ['Year', 'Month']]]
        logger.info(f'Fetching data for {month.strftime("%B %Y")} for reconciliations')
        return df
    else:
        logger.error(f'Error fetching data for {month.strftime("%B %Y")}: {response.status_code}')
        return None


def fetch_all_Reconciliations_data_parallel():
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

    csv_file_path = os.path.join(folder_path, Reconciliations)
    final_df.to_csv(csv_file_path, index=False)

    logger.info(f'CSV created in {csv_file_path}')


fetch_all_Reconciliations_data_parallel()
generate_update_schema()
upload_csv()

# def fetch_all_Reconciliations_data():
#     all_dfs = []
#
#     current_month = start_date
#     while current_month <= end_date:
#         url = f'{base_url}/reconciliations?filter[month]={current_month.strftime("%B")}&filter[year]={current_month.year}'
#
#         headers = {
#             'x-api-key': access_token
#         }
#
#         # Check if the current month is still within the desired range
#         if current_month <= end_date:
#             response = requests.get(url, headers=headers)
#             if response.status_code == 200:
#                 data = response.json()['data']
#                 flattened_data_list = [flatten(item, separator='_') for item in data]
#                 df = pd.DataFrame(flattened_data_list)
#                 df["Month"] = current_month.strftime("%B")
#                 df['Year'] = current_month.year
#                 df = df[['Year', 'Month'] + [col for col in df.columns if col not in ['Year', 'Month']]]
#                 all_dfs.append(df)
#                 print(f'Fetching data for {current_month.strftime("%B %Y")} for reconciliations')
#             else:
#                 print(response.status_code)
#
#         current_month = current_month + relativedelta(months=1)
#
#     # Move these lines outside the loop to create the folder and write the CSV file once
#     folder_path = output_dir
#     if not os.path.exists(folder_path):
#         os.makedirs(folder_path)
#
#     # Concatenate all DataFrames into a single DataFrame
#     final_df = pd.concat(all_dfs, ignore_index=True)
#
#     # Save the final DataFrame to CSV
#     csv_file_path = os.path.join(folder_path, Reconciliations)
#     final_df.to_csv(csv_file_path, index=False)
#
#
# fetch_all_Reconciliations_data()
# generate_update_schema()
# upload_csv()
