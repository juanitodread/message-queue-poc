import random

import pika
from pika.connection import ConnectionParameters
from datetime import datetime
from datawarehouse.bigquery.bigquery import BigQuery
from model.message import Message

HOST = 'localhost'


class Consumer:
    def __init__(self):
        self._connection = pika.BlockingConnection(ConnectionParameters(host=HOST))

    def consume(self, queue):
        channel = self.get_channel()

        channel.exchange_declare(
            exchange=queue,
            exchange_type='fanout'
        )

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(
            exchange=queue,
            queue=queue_name
        )

        channel.basic_consume(
            self._process_message,
            queue=queue,
            no_ack=True
        )

        print(f'Starting consumer => exchange: {queue}, queue: {queue_name}')
        channel.start_consuming()

    def _process_message(self, ch, method, properties, message):
        print(f'A new message was received: {message}')
        msg_type = ['incoming', 'outgoing']

        storage = BigQuery()
        message = Message(f'1', '1', random.choice(msg_type), message.decode('utf-8'))
        storage.insert_message(message)
        print('message send at ', datetime.utcnow())

    def get_channel(self):
        return self._connection.channel()

    def open(self) -> None:
        self._connection = pika.BlockingConnection(ConnectionParameters(HOST))

    def close(self) -> None:
        self._connection.close()
