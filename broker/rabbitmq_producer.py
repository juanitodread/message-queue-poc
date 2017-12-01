from pika.adapters.blocking_connection import BlockingConnection
from pika.connection import ConnectionParameters


HOST = 'localhost'


class Producer:
    def __init__(self):
        self._connection = BlockingConnection(ConnectionParameters(HOST))

    def produce(self, message: str, queue: str) -> None:
        channel = self.get_channel()

        channel.exchange_declare(
            exchange=queue,
            exchange_type='fanout'
        )

        channel.basic_publish(
            exchange=queue,
            routing_key='',
            body=message
        )
        print(f'Message: {message} published on queue: {queue}')

    def get_channel(self):
        return self._connection.channel()

    def open(self) -> None:
        self._connection = BlockingConnection(ConnectionParameters(HOST))

    def close(self) -> None:
        self._connection.close()
