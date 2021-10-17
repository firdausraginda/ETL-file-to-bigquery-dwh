import pandas as pd
from bigquery.setup import create_bq_client
import pandas_gbq


client = create_bq_client()
dataset_name = "project_1_staging"


def sql_read_query(table_name):
    """define sql query to read table with given table name"""

    sql = """
        SELECT *
        FROM `dummy-329203.project_1_staging.{}`
        """.format(table_name)

    return sql


def transform_precipitation():

    # get sql query to read table
    table_name = 'precipitation_inch'
    sql = sql_read_query(table_name)

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)

    # convert date from int to datetime
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

    # convert precipitation from object to float with error handling to NaN
    df['precipitation'] = pd.to_numeric(df['precipitation'], errors='coerce')

    print(df)
    print(df.info())

    return df


if __name__ == "__main__":
    
    transform_precipitation()