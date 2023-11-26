
import os
from time import sleep
from google.cloud import bigquery
from dependencies.static_variable import StaticVariable

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = StaticVariable.service_key_path


# 1. Create a BigQuery client
client = bigquery.Client()


# 2. Create reference and table
dataset_reference   = bigquery.DatasetReference(
        project     = client.project,
        dataset_id  = '<dataset_id>'
    )

table_reference     = bigquery.TableReference(
        dataset_ref = dataset_reference,
        table_id    = '<table_name>'
    )

bigquery_table      = client.get_table(table_reference)


# 3. Get a snapshot of bigquery table schema
bigquery_table_schema_copy = bigquery_table.schema.copy()


# 4. Add new schema into table
new_columns = [
    bigquery.SchemaField('description', 'STRING', mode='NULLABLE')
]

bigquery_table.schema = bigquery_table_schema_copy + new_columns

client.update_table(bigquery_table, fields=['schema'])


# 5. Drop table colum
drop_column_query = client.query('''
    ALTER TABLE <table_identifier>
    DROP COLUMN IF EXISTS description;
''')

query_response = client.query(drop_column_query)

while query_response.state != 'DONE':
    print('Waiting for job to finish...')
    sleep(3)
    query_response.reload()
