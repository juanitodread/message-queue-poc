import falcon

from api.analytics import MessageMetric
from api.consumer import MessageConsumerResource
from api.producer import MessageProducerResource


class App(falcon.API):
    def __init__(self, *args, **kwargs):
        super(App, self).__init__(*args, **kwargs)
        self._configure_routes()

    def _configure_routes(self):
        self.add_route(
            '/api/message/producer',
            MessageProducerResource()
        )
        self.add_route(
            '/api/message/start-consumer',
            MessageConsumerResource()
        )
        self.add_route(
            '/api/analytics/message-metrics',
            MessageMetric()
        )
