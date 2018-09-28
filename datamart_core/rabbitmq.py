import logging
import os
import pika


logger = logging.getLogger(__name__)


class RabbitMQ(object):
    def __init__(self):
        self._amqp = None
        self._amqp_channel = None
        self.__connect()

    def __connect(self):
        self._amqp = pika.adapters.AsyncioConnection(
            pika.ConnectionParameters(
                host=os.environ['AMQP_HOST'],
                credentials=pika.PlainCredentials(os.environ['AMQP_USER'],
                                                  os.environ['AMQP_PASSWORD']),
            ),
            self.__on_connection_open,
        )

    def __on_connection_open(self):
        logger.info("Connected to RabbitMQ")
        self._amqp.add_on_close_callback(self.on_connection_closed)
        self._amqp.channel(on_open_callback=self.__on_channel_open)

    def __on_channel_open(self, channel):
        self._amqp_channel = channel
        self._amqp_channel.add_on_close_callback(self.__on_channel_closed)
        self.on_channel_open()

    def on_channel_open(self):
        pass

    def __on_channel_closed(self, channel, reply_code, reply_text):
        logger.info("Channel was closed: (%s) %s", reply_code, reply_text)
        self._amqp_channel = None
        self._amqp.close()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._amqp_channel = None
        logger.info("Connection was closed: (%s) %s", reply_code, reply_text)
        self._amqp.add_timeout(5, self.__connect)
