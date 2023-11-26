import os
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


# 3. Create (or) Update label to your table
bigquery_table.labels = {'country': 'canada', 'category': 'partial_error'}

client.update_table(bigquery_table, fields=['labels'])


# 4. Delete labels of your table
for key, val in bigquery_table.labels.items():
  bigquery_table.labels[key] = None

client.update_table(bigquery_table, fields=['labels'])
