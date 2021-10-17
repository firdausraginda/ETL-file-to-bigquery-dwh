from pathlib import Path
import os
from bigquery.setup import create_bq_client
from google.cloud import bigquery


current_path = Path(__file__).absolute()
path_to_files = current_path.parent.joinpath("sql_transforms/")
list_files = os.listdir(path_to_files)

for file in list_files:

    # set complete path to sql file
    sql_file = open(path_to_files.joinpath(file))
    sql_query = sql_file.read()

    # define dataset & table name
    dataset_name = "project_1_ods"
    table_name = file.replace(".sql", "")

    # get bigquery client
    client = create_bq_client()

    # set job config: destination table & write disposition 
    # write disposition is action took if dest table alr exists: fail (WRITE_EMPTY), replace (WRITE_TRUNCATE), or append (WRITE_APPEND)
    job_config = bigquery.QueryJobConfig(
        destination=f"{client.project}.{dataset_name}.{table_name}", 
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

    # make API request to run the job config
    query_job = client.query(sql_query, job_config=job_config)
    query_job.result()