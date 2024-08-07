from all_pull_checklist import fetch_all_Checklists_data_parallel
from all_pull_reconciliations import fetch_all_reconciliations_data_parallel
from upload_to_azure import upload_all_files_to_azure_blob
from log_to_csv import convert_to_csv
from upload_log_csv import generate_update_schema, upload_csv

if __name__ == "__main__":
    try:
        fetch_all_Checklists_data_parallel()
        fetch_all_reconciliations_data_parallel()
        upload_all_files_to_azure_blob()
    except Exception as e:
        print("Exception occurred: ", e)
    finally:
        convert_to_csv()
        generate_update_schema()
        upload_csv()
