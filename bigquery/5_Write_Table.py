import os
from google.cloud import bigquery
from dependencies.static_variable import StaticVariable

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = StaticVariable.service_key_path


# 1. Create a BigQuery client
client = bigquery.Client()


# 2. Define source and destination
dataset_id        = '<dataset_id>'
source_table_name = '<source_table_name>'
target_table_name = '<target_table_name>'


# 3. Construct source and destination references
source_reference      = client.dataset(dataset_id).table(source_table_name)
destination_reference = client.dataset(dataset_id).table(target_table_name)


# 4. Make an API request
select_query = "SELECT * FROM <table_identifier> LIMIT 2;"
query_job    = client.query(select_query)


# 5. Wait for query job to complete
query_job.result()


# 6. Get and Write DataFrame into destination table
results_df  = query_job.to_dataframe()
load_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)
load_job    = client.load_table_from_dataframe(results_df, destination_reference, job_config=load_config)


# 7. Wait for load job to complete
load_job.result()