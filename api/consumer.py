import falcon

from broker.rabbitmq_consumer import Consumer


class MessageConsumerResource:
    def __init__(self):
        self._consumer = Consumer()

    def on_get(self, req, res):
        queue = req.get_param('queue', 'default')

        print(f'Starting consumer for queue: {queue}...')
        self._consumer.consume(queue)

        res.status = falcon.HTTP_200
