import falcon

from broker.rabbitmq_producer import Producer


class MessageProducerResource:
    def __init__(self):
        self._producer = Producer()

    def on_get(self, req, res):
        msg = req.get_param('msg', 'default')
        queue = req.get_param('queue', 'default')
        self._producer.produce(msg, queue)

        res.status = falcon.HTTP_200
