import os
import pandas as pd
import requests
from config.env import flo_base, access_token
from flatten_json import flatten
from commons import output_dir, Checklist_file, Reconciliations

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)


def fetch_CheckList_data():
    url = f'{flo_base}/checklists?filter[month]=December&filter[year]=2023'

    headers = {
        'x-api-key': access_token
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()['data']
        flattened_data_list = [flatten(item, separator='_') for item in data]
        df = pd.DataFrame(flattened_data_list)
        folder_path = output_dir
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            csv_file_path = os.path.join(folder_path, Checklist_file)
            df.to_csv(csv_file_path, index=False)

    else:
        print(response.status_code)


fetch_CheckList_data()

# def fetch_Reconciliations_data():
#     url = f'{flo_base}/reconciliations?filter[month]=December&filter[year]=2023'
#
#     headers = {
#         'x-api-key': access_token
#     }
#
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         data = response.json()['data']
#         flattened_data_list = [flatten(item, separator='_') for item in data]
#         df = pd.DataFrame(flattened_data_list)
#         # print(df)
#         folder_path = output_dir
#         csv_file_path = os.path.join(folder_path, Reconciliations)
#         df.to_csv(csv_file_path, index=False)
#
#     else:
#         print(response.status_code)
#
#
# fetch_Reconciliations_data()
