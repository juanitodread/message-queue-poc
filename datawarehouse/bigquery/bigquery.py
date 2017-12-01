from google.cloud import bigquery


class SimpleSchemaColumn:
    def __init__(self, column_name, column_type, required=True):
        self._column_name = column_name
        self._column_type = column_type
        self._required = required

    @property
    def column_name(self):
        return self._column_name

    @property
    def column_type(self):
        return self._column_type

    @property
    def is_required(self):
        return self._required

    @property
    def mode(self):
        return 'required' if self._required else ''


class BigQuery:
    def __init__(self):
        self._bigquery_client = bigquery.Client()
        self._query_seconds_timeout = 10

    def create_dataset(self, name):
        dataset_ref = self._bigquery_client.dataset(name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset = self._bigquery_client.create_dataset(dataset)
        return dataset

    def get_dataset(self, name):
        dataset_ref = self._bigquery_client.dataset(name)
        return bigquery.Dataset(dataset_ref)

    def delete_dataset(self, name):
        self._bigquery_client.delete_dataset(name)

    def list_tables(self, dataset):
        return list(self._bigquery_client.list_dataset_tables(dataset))

    def create_table(self, dataset, table_name, columns):
        schema = [
            bigquery.SchemaField(column.column_name, column.column_type, mode=column.mode)
            for column in columns
        ]
        print('Schema: ')
        print(schema)

        table_ref = dataset.table(table_name)
        table = bigquery.Table(table_ref, schema=schema)
        table = self._bigquery_client.create_table(table)

        return table

    def insert_message(self, message):
        dataset = self.get_dataset('test_analytics')
        table_ref = dataset.table('message')
        table = bigquery.Table(table_ref)
        table = self._bigquery_client.get_table(table)

        row = [
            (message.bot_id, message.user_id, message.type, message.content, message.timestamp),
        ]
        errors = self._bigquery_client.create_rows(table, row)

        if errors:
            raise Exception(errors)

    def run_query(self, query, params):
        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = params if params and len(params) > 0 else []

        query_job = self._bigquery_client.query(query, job_config=job_config)

        query_job.result(timeout=self._query_seconds_timeout) # Waits for the query to finish
        destination_table_ref = query_job.destination
        table = self._bigquery_client.get_table(destination_table_ref)
        result = self._bigquery_client.list_rows(table)
        return result

