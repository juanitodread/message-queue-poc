from google.cloud import bigquery
from datawarehouse.bigquery.bigquery import BigQuery


class MessageMetric:
    def on_get(self, req, res):
        query = (
            'select count(bot_id) count '
            'from `bot-analytics.test_analytics.message` '
            'where bot_id = @bot_id '
            'group by bot_id'
        )

        bot_id = req.get_param('bot_id', '1')

        bot_id_param = bigquery.ScalarQueryParameter('bot_id', 'STRING', bot_id)

        storage = BigQuery()
        result = storage.run_query(query, [bot_id_param])

        print(result)
        message_count = 0
        for row in result:
            message_count = row.count

        response = {
            'message_count': message_count
        }
        print(str(response))
        res.body = str(response)
