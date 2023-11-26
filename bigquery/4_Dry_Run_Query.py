import os
from google.cloud import bigquery
from dependencies.static_variable import StaticVariable

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = StaticVariable.service_key_path


# 1. Create a BigQuery client
client = bigquery.Client()


# 2. Run a query
select_query = "SELECT * FROM <table_identitfier>;"


# 3. Configure job
job_config = bigquery.QueryJobConfig(dry_run = True, use_query_cache = False)


# 4. Make an API request
query_job = client.query(select_query, job_config)

print(f"Total bytes processed: {query_job.total_bytes_processed}")
