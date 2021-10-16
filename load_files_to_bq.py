from google.cloud import bigquery
import pandas as pd

table_id = 'my_dataset.new_table'

df.to_gbq(table_id)