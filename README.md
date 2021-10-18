# ETL-file-to-bigquery-dwh

## Depedencies

Kindly install depedencies using **pipenv** as depedency manager.

For automatic dependency installation, can just run:
```
pipenv sync
```

If still encounter **ModuleNotFoundError**, can do it manually instead:

- install pandas
```
pipenv install pandas
```

- install [google auth oauthlib](https://cloud.google.com/docs/authentication/end-user)
```
pipenv install google-auth-oauthlib
```

- install [google cloud bigquery](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python)
```
pipenv install google-cloud-bigquery
```

- install [pandas gbq](https://pandas-gbq.readthedocs.io/en/latest/install.html)
```
pipenv install pandas-gbq
```

## Usage

First, need to create empty dataset for **staging**, **ODS**, & **DWH**:
```
pipenv run python bigquery/setup.py
```

To convert all **json** files to **csv** files, and write it to **staging dataset**, can run the `file_source_to_staging.py` file:
```
pipenv run python file_source_to_staging.py
```

SQL scripts that transform data to **ODS dataset** is stored under `./sql_transforms_to_ods/` folder, and for **DWH dataset** stored under `./sql_transforms_to_dwh` folder. These SQL scripts are executed by python file `run_sql_query.py`:
```
pipenv run python run_sql_query.py
```

## References
- [authenticating with a service account key file](https://cloud.google.com/bigquery/docs/authentication/service-account-file)