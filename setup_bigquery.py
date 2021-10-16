from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
 
current_path = Path(__file__).absolute()
credentials = service_account.Credentials.from_service_account_file(
    current_path.parent.parent.joinpath("/service_account.json"), scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)