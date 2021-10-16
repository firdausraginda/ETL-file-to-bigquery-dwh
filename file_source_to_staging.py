from google.cloud import bigquery
import pandas as pd
from bigquery.setup import create_bq_client
import pandas_gbq
from pathlib import Path
import os


def read_csv_file(path_to_csv):
    """read csv file and do minor data type conversion, return it as dataframe"""

    split_path = str(path_to_csv).split("-")
    file_name = split_path[-2].lower() + "-" + split_path[-1].lower()
    file_name = file_name.replace("-", "_").replace(".csv", "")

    df = pd.read_csv(path_to_csv, na_values="T", dtype={"precipitation": float}, parse_dates=["date"])
    
    return df, file_name


def read_json_file(path_to_json):
    """read json file and return it as dataframe"""

    df = pd.read_json(path_to_json, lines=True)

    return df


def write_to_staging(df, table_name):
    """write data from csv/json file to staging dataset with respective table name"""

    client = create_bq_client()
    dataset_name = "project_1_staging"

    pandas_gbq.to_gbq(
        df, f"{dataset_name}.{table_name}", project_id=client.project, if_exists="replace"
    )

    return None


if __name__ == "__main__":

    current_path = Path(__file__).absolute()

    # get path to csv files
    path_to_csv = current_path.parent.joinpath("src/weather")
    list_csv = os.listdir(path_to_csv)

    # get path to json files
    path_to_json = current_path.parent.joinpath("src/yelp_reviews")
    list_json = os.listdir(path_to_json)
    
    for csv in list_csv:
        complete_path = path_to_csv.joinpath(csv)
        csv_df, csv_table_name = read_csv_file(complete_path)
        write_to_staging(csv_df, csv_table_name)