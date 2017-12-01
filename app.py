from api.server import App
from datawarehouse.bigquery.bigquery import BigQuery, SimpleSchemaColumn


bigquery = BigQuery()
# bigquery.create_dataset('test_analytics')
test_analytics_dataset = bigquery.get_dataset('test_analytics')

# Creating a table
columns = [
    SimpleSchemaColumn('bot_id', 'string'),
    SimpleSchemaColumn('user_id', 'string'),
    SimpleSchemaColumn('type', 'string'),
    SimpleSchemaColumn('content', 'string')
]

#bigquery.create_table(test_analytics_dataset, 'message', columns)

tables = bigquery.list_tables(test_analytics_dataset)
print('Tables: ')
for table in tables:
    print(table.table_id)

# bigquery.delete_dataset(test_analytics_dataset)

api = App()
