import datetime
import time
import os
from api.server import App
from datawarehouse.bigquery.bigquery_wrapper import BigQueryWrapper


ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
bigquery = BigQueryWrapper(ROOT_PATH + '/service-account.json')

DATA_SET = 'test_analytics'
print('BigQuery datasets: ')
datasets = bigquery.get_datasets()
for dataset in datasets:
    print(dataset)

print('BigQuery table message in "test_analytics" dataset: ')
message_table = bigquery.get_table(DATA_SET, 'message')
print(message_table)


print('Create a new table "test" in "test_analytics" dataset in BigQuery')
schema = [
    {'name': 'id', 'type': 'string', 'mode': 'nullable'},
    {'name': 'name', 'type': 'string', 'mode': 'nullable'},
    {'name': 'created_at', 'type': 'timestamp', 'mode': 'nullable'}
]

if bigquery.table_exists(DATA_SET, 'test'):
    bigquery.drop_table(DATA_SET, 'test')

print('Create a new table "test" in "test_analytics" dataset with the schema: ')
print(schema)
new_table = bigquery.create_table(DATA_SET, 'test', schema)
print(new_table)

test_table = bigquery.get_table(DATA_SET, 'test')
print('BigQuery table "test_table" in "test_analytics" dataset: ')
print(test_table)

test_row1 = {
    'id': str(time.time()),
    'name': 'john',
    'created_at': datetime.datetime.utcnow()
}

print('Insert a new test row: ')
print(test_row1)

insert_result = bigquery.insert(DATA_SET, 'test', test_row1)
print('Result after insert: ' + str(insert_result))


print('Run a query in message table: ')
QUERY = 'select * from test_analytics.message limit 50'
results = bigquery.run_query(QUERY, timeout=10)
for result in results:
    print('Result: ' + str(result))

api = App()
