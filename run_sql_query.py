from pathlib import Path
import os
from bigquery.setup import create_bq_client
from google.cloud import bigquery


def execute_sql(path_to_sql_folder, dataset_destination_name, insert_method):
    current_path = Path(__file__).absolute()
    path_to_files = current_path.parent.joinpath(f"{path_to_sql_folder}/")
    list_files = os.listdir(path_to_files)

    for file in list_files:

        # set complete path to sql file
        sql_file = open(path_to_files.joinpath(file))
        sql_query = sql_file.read()

        # define dataset & table name
        dataset_name = dataset_destination_name
        table_name = file.replace(".sql", "")

        # get bigquery client
        client = create_bq_client()

        # set the write dispotition
        # write disposition is action took if dest table alr exists
        if insert_method == 'new':
            write_dispotition = bigquery.WriteDisposition.WRITE_EMPTY
        elif insert_method == 'replace':
            write_dispotition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif insert_method == 'append':
            write_dispotition = bigquery.WriteDisposition.WRITE_APPEND

        # set job config: destination table & write disposition 
        job_config = bigquery.QueryJobConfig(
            destination=f"{client.project}.{dataset_name}.{table_name}", 
            write_disposition=write_dispotition
            )

        # make API request to run the job config
        query_job = client.query(sql_query, job_config=job_config)
        query_job.result()


if __name__ == "__main__":

    # execute sql transforms from staging to ods
    execute_sql("sql_transforms_to_ods", "project_1_ods", "replace")

    # execute sql transforms from ods to dwh
    execute_sql("sql_transforms_to_dwh", "project_1_dwh", "append")