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

    # convert precipitation & precipitation_normal to float with error handling to NaN
    df['precipitation'] = pd.to_numeric(df['precipitation'], errors='coerce')
    df['precipitation_normal'] = pd.to_numeric(df['precipitation_normal'], errors='coerce')

    return df


def transform_temperature():

    # get sql query to read table
    table_name = 'temperature_degreef'
    sql = sql_read_query(table_name)

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)

    # convert date from int to datetime
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

    # convert min, max, normal_min, & normal_max to float with error handling to NaN
    df['min'] = pd.to_numeric(df['min'], errors='coerce')
    df['max'] = pd.to_numeric(df['max'], errors='coerce')
    df['normal_min'] = pd.to_numeric(df['normal_min'], errors='coerce')
    df['normal_max'] = pd.to_numeric(df['normal_max'], errors='coerce')

    return df


if __name__ == "__main__":
    
    precipitation_df = transform_precipitation()
    temperature_df = transform_temperature()

    print(temperature_df)
    print(temperature_df.info())