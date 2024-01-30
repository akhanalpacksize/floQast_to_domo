import os

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from config.env import az_account_name, az_account_key, az_container_name, az_connection_string
from commons import output_dir
import uuid

def upload_all_files_to_azure_blob():
    try:
        # Initialize Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(az_connection_string)

        # Get the list of files in the output_dir
        file_list = os.listdir(output_dir)

        for file_name in file_list:
            # Get the BlobClient for the file
            blob_client = blob_service_client.get_blob_client(container=az_container_name, blob=file_name)

            # Upload the file to Azure Blob Storage
            with open(os.path.join(output_dir, file_name), 'rb') as f:
                block_list = []
                CHUNK_SIZE = 1024 * 1024 * 4
                for read_data in iter(lambda: f.read(CHUNK_SIZE), b''):
                    blk_id = str(uuid.uuid4())
                    blob_client.stage_block(block_id=blk_id, data=read_data)
                    block_list.append(BlobBlock(block_id=blk_id))
                blob_client.commit_block_list(block_list)

            print(f"Upload to Azure: {file_name}")

    except Exception as err:
        print(f'AZURE BLOB UPLOAD ERROR: {err}')
