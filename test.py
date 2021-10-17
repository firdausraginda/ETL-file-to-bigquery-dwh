from pathlib import Path
import os
from bigquery.setup import create_bq_client
import pandas_gbq

current_path = Path(__file__).absolute()
path_to_files = current_path.parent.joinpath("sql_transforms/")
list_files = os.listdir(path_to_files)

for list_file in list_files:

    # set complete path to sql file
    sql_file = open(path_to_files.joinpath(list_file))
    sql_query = sql_file.read()

    # get bigquery client
    client = create_bq_client()

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql_query, project_id=client.project)

print(df)
print(df.info())