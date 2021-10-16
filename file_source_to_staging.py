from google.cloud import bigquery
import pandas as pd
from bigquery.setup import create_bq_client
import pandas_gbq
from pathlib import Path
import os


def read_csv_file(path_to_csv):
    """read csv file and do minor data type conversion, return it as dataframe"""

    # get file name
    split_path = str(path_to_csv).split("-")
    file_name = split_path[-2].lower() + "-" + split_path[-1].lower()
    file_name = file_name.replace("-", "_").replace(".csv", "")

    dfs = pd.read_csv(path_to_csv, na_values="T", dtype={"precipitation": float}, parse_dates=["date"], chunksize=10000)
    
    for df in dfs:
        yield df, file_name


def read_json_file(path_to_json):
    """read json file and return it as dataframe"""

    # get file name
    split_path = str(path_to_json).split("_")
    file_name = "yelp_" + split_path[-1].lower()
    file_name = file_name.replace(".json", "")

    dfs = pd.read_json(path_to_json, lines=True, chunksize=10000)
    
    for df in dfs:
        yield df, file_name


def write_to_staging(dfs):
    """write data from csv/json file to staging dataset with respective table name"""

    client = create_bq_client()
    dataset_name = "project_1_staging"

    for df in dfs:
        pandas_gbq.to_gbq(
            df[0], f"{dataset_name}.{df[1]}", project_id=client.project, if_exists="append"
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
    
    # read all csv files and write it to staging
    for csv in list_csv:
        complete_path = path_to_csv.joinpath(csv)
        csv_df = read_csv_file(complete_path)
        write_to_staging(csv_df)

    # read all json files and write it to staging
    # for json in list_json:
    #     complete_path = path_to_json.joinpath(json)
    #     json_df = read_json_file(complete_path)
    #     write_to_staging(json_df)
    
    #     break