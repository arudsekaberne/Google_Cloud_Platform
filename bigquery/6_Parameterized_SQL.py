import os
from datetime import datetime
from google.cloud import bigquery
from dependencies.static_variable import StaticVariable

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = StaticVariable.service_key_path


# 1. Create a BigQuery client
client = bigquery.Client()


# 2. Inserting scalar values
scalar_query = '''
  SELECT * FROM <table_identifier>
  WHERE account_number > @number;
'''

scalar_query_parameters = [
    bigquery.ScalarQueryParameter('number', 'BIGNUMERIC', 500)
]

scalar_job_config = bigquery.QueryJobConfig(query_parameters = scalar_query_parameters)
scalar_query_job  = client.query(scalar_query, job_config = scalar_job_config)
scalar_query_job.to_dataframe()


# 3. Inserting datetime values
datetime_query = '''
  SELECT * FROM <table_identifier>
  WHERE rec_ins_time < @datetime;
'''

datetime_query_parameters = [
    bigquery.ScalarQueryParameter('datetime', 'DATETIME', datetime(2023, 11, 26))
]

datetime_job_config = bigquery.QueryJobConfig(query_parameters = datetime_query_parameters)
datetime_query_job  = client.query(datetime_query, job_config = datetime_job_config)
datetime_query_job.to_dataframe()


# 4. Inserting on positional basics
positional_query = '''
  SELECT * FROM <table_identifier>
  WHERE account_number < ? AND LENGTH(postal_code) = ?;
'''

positional_query_parameters = [
    bigquery.ScalarQueryParameter(None, 'BIGNUMERIC', 500),
    bigquery.ScalarQueryParameter(None, 'INT64', 4)
]

positional_job_config = bigquery.QueryJobConfig(query_parameters = positional_query_parameters)
positional_query_job  = client.query(positional_query, job_config = positional_job_config)
positional_query_job.to_dataframe()


# 5. Working with Array values
array_query = '''
  SELECT * FROM <table_identifier>
  WHERE account_number IN UNNEST(@account_list);
'''

array_query_parameters = [
    bigquery.ArrayQueryParameter('account_list', 'BIGNUMERIC', [459])
]

array_job_config = bigquery.QueryJobConfig(query_parameters = array_query_parameters)
array_query_job  = client.query(array_query, job_config = array_job_config)
array_query_job.to_dataframe()


# 6. Working with Struct Type
struct_query = '''
  SELECT @structs AS struct_col
'''

struct_query_parameters = [
    bigquery.StructQueryParameter(
        'structs',
        bigquery.ScalarQueryParameter('id', 'INT64', '1'),  
        bigquery.ScalarQueryParameter('name', 'STRING', 'arudsekaberne'),    
    )
]

struct_job_config = bigquery.QueryJobConfig(query_parameters = struct_query_parameters)
struct_query_job  = client.query(struct_query, job_config = struct_job_config)
struct_query_job.to_dataframe()