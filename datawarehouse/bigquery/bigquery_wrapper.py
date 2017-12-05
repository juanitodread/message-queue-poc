from bigquery import get_client


class BigQueryWrapper:
    def __init__(self, account_token_file):
        self._bigquery_client = get_client(
            json_key_file=account_token_file,
            readonly=True
        )

    def get_datasets(self):
        return self._bigquery_client.get_datasets()

    def get_dataset(self, dataset_name):
        return self._bigquery_client.get_dataset(dataset_name)

    def create_dataset(self, dataset_name, description):
        return self._bigquery_client.create_dataset(dataset_name, description)

    def dataset_exists(self, dataset_name):
        return self._bigquery_client.check_dataset(dataset_name)

    def create_table(self, dataset_name, table_name, schema):
        return self._bigquery_client.create_table(
            dataset_name,
            table_name,
            schema
        )

    def drop_table(self, dataset_name, table_name):
        return self._bigquery_client.delete_table(dataset_name, table_name)

    def get_table(self, dataset_name, table_name):
        return self._bigquery_client.get_table(dataset_name, table_name)

    def table_exists(self, dataset_name, table_name):
        return self._bigquery_client.check_table(dataset_name, table_name)

    def insert(self, dataset_name, table_name, rows):
        return self._bigquery_client.push_rows(dataset_name, table_name, rows)

    def run_query(self, query, timeout):
        job_id, _results = self._bigquery_client.query(query, timeout=timeout)
        return self._bigquery_client.get_query_rows(job_id)

    def is_query_done(self, job_id):
        complete = self._bigquery_client.check_job(job_id)
        return complete


