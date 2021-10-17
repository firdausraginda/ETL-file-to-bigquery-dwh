from google.cloud import bigquery
import pandas as pd
from bigquery.setup import create_bq_client
import pandas_gbq
from pathlib import Path
import os


def convert_json_to_csv(path_to_json):
    """read json file and write as csv in chunks"""

    # get file name
    split_path = str(path_to_json).split("_")
    file_name = "yelp_" + split_path[-1].lower()
    file_name = file_name.replace(".json", "")

    # read json and write to csv per chunksize
    batch_no = 1
    for chunk_df in pd.read_json(path_to_json, lines=True, chunksize=100000):
        chunk_df.to_csv(f"./src/{file_name}_{batch_no}.csv", index=None)
        batch_no += 1

    return None


def read_csv_file(path_to_csv):
    """read csv file and do minor data type conversion, return it as dataframe"""

    # get file name for weather csv files
    if "yelp" not in str(path_to_csv):
        split_path = str(path_to_csv).split("-")
        file_name = split_path[-2].lower() + "_" + split_path[-1].lower()
        file_name = file_name.replace(".csv", "")    
    # get file name for yelp csv files
    else:
        split_path = str(path_to_csv).split("_")
        file_name = "yelp_" + split_path[-2].lower()
        file_name = file_name.replace(".csv", "")

    # yield csv dataframe per chunk
    for chunk_df in pd.read_csv(path_to_csv, chunksize=100000):
        yield chunk_df, file_name


def write_to_staging(chunk_generator):
    """write data from csv/json file to staging dataset with respective table name"""

    client = create_bq_client()
    dataset_name = "project_1_staging"

    # write csv dataframe to staging per chunk
    for chunk_df in chunk_generator:
        pandas_gbq.to_gbq(
            chunk_df[0], f"{dataset_name}.{chunk_df[1]}", project_id=client.project, if_exists="append"
        )

    return None


if __name__ == "__main__":

    current_path = Path(__file__).absolute()

    # get source files
    path_to_files = current_path.parent.joinpath("src/")
    list_files = os.listdir(path_to_files)
    
    # read all json files and write it as csv
    for file in list_files:
        if 'business' in file or 'checkin' in file: # debugging purpose
            if file.endswith(".json"):
                complete_path = path_to_files.joinpath(file)
                convert_json_to_csv(complete_path)

    # get source files after adding new csv files
    path_to_files = current_path.parent.joinpath("src/")
    list_files = os.listdir(path_to_files)
    
    # read all csv files and write to staging
    for file in list_files:
            if file.endswith(".csv"):
                complete_path = path_to_files.joinpath(file)
                csv_df = read_csv_file(complete_path)
                write_to_staging(csv_df)