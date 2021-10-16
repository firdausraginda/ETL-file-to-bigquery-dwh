from google.cloud import bigquery
import pandas as pd
from bigquery.setup import create_bq_client
import pandas_gbq


precipitation_df = pd.read_csv("./src/weather/USW00023169-LAS_VEGAS_MCCARRAN_INTL_AP-precipitation-inch.csv",
                               na_values="T", dtype={"precipitation": float}, parse_dates=["date"])

temperature_df = pd.read_csv(
    "./src/weather/USW00023169-temperature-degreeF.csv", parse_dates=["date"])

# print(precipitation_df.info())
# print(temperature_df.info())

client = create_bq_client()
dataset_name = "project_1_staging"
table_name = "precipitation"

pandas_gbq.to_gbq(
    precipitation_df, f"{dataset_name}.{table_name}", project_id=client.project, if_exists="replace"
)