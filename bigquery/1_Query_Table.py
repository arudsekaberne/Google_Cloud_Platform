import os
from google.cloud import bigquery
from dependencies.static_variable import StaticVariable


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = StaticVariable.service_key_path


# 1. Create a BigQuery client
client = bigquery.Client()


# 2. Write a query
select_query = "SELECT * FROM <table_identitifer> LIMIT 10;"


# 3. Make an API request
query_response = client.query(select_query)


# 4. Iterator
for row in query_response.result():

    # Use index position (or) dot notation to access column
    print(row[0], row.account_number)
