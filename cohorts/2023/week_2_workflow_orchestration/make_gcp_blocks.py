import os

from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from dotenv import load_dotenv

load_dotenv()

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!

service_account_file = "~/.secrets/prefect-service-account.json"
credentials_block = GcpCredentials(service_account_file=os.environ["GCP_CREDS_FILES"])
credentials_block.save("zoom-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
    bucket=os.environ["GCS_BUCKET"],  # insert your  GCS bucket name
)

bucket_block.save("zoom-gcs", overwrite=True)
