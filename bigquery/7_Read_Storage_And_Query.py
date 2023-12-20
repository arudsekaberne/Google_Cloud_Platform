import os
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from dependencies.static_variable import StaticVariable

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = StaticVariable.service_key_path


# 1. Create a BigQuery and Storage client
bigquery_client = bigquery.Client()
storage_client  = storage.Client()


# Specify the bucket and file path
bucket_name = "<bucket_name>"
file_path   = "<file_path>"


# Get the file content
bucket    = storage_client.get_bucket(bucket_name)
blob      = bucket.blob(file_path)
sql_query = blob.download_as_text()


# Execute the query
query_job = bigquery_client.query(sql_query)

for row in query_job.result():
    print(row)

