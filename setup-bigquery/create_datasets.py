from google.oauth2 import service_account
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
 
current_path = Path(__file__).absolute()
credentials = service_account.Credentials.from_service_account_file(
    current_path.parent.parent.joinpath("service_account.json"), scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# set dataset names
dataset_names = ['project_1_staging', 'project_1_ods', 'project_1_dwh']

# create dataset
for dataset_name in dataset_names:
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)