import asyncio
import socket
from threading import Thread

import confluent_kafka as kafka
from confluent_kafka import KafkaException

from event_bus.models import AppSettings, KafkaProducerCredentials, LoggerSettings

logger = LoggerSettings().logger


class Producer:

    def __init__(self, settings: dict):
        self.kafka_settings = settings
        self.producer = kafka.Producer(self.kafka_settings)

    def produce(self, topic, key, value, callback=None, **kwargs):
        """
        py:function::  produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])

        Produce message to topic.
        This is an asynchronous operation, an application may use the
        ``callback`` (alias ``on_delivery``) argument to pass a function
        (or lambda) that will be called from :py:func:`poll()` when the
        message has been successfully delivered or permanently fails delivery.

        :param func on_delivery(err,msg): Delivery report callback to call
        (from :py:func:`poll()` or :py:func:`flush()`) on successful or failed delivery
        :param int timestamp: Message timestamp (CreateTime) in milliseconds since epoch UTC.
            Default value is current time.
        :param dict|list headers: Message headers to set on the message.
            The header key must be a string while the value must be binary, unicode or None.
            Accepts a list of (key,value) or a dict.
        """
        if not callback:
            callback = self.acked

        self.producer.produce(topic, key=key, value=value, on_delivery=callback, **kwargs)
        # self.producer.poll(1)
        """
        Ensure the message is sent immediately
        Wait for all messages in the Producer queue to be delivered.
        This is a convenience method that calls `poll()` until
        `len()` is zero or the optional timeout elapses.
        """
        return self.producer.flush(1)

    @staticmethod
    def acked(err, msg):
        if err is not None:
            logger.error(f'Failed to deliver message: {msg}, error: {err}')
        else:
            logger.info(f'Message produced: {msg.value()}')


class AsyncProducer(Producer):
    _cancelled = False

    def start(self):
        self.__loop = asyncio.get_event_loop()
        self.__poll_thread = Thread(target=self.__poll_loop)
        self.__poll_thread.start()
        self.producer = kafka.Producer(self.kafka_settings)
        return self

    def __poll_loop(self):
        while not self._cancelled:
            yield self.producer.flush(1.0)
            # self.producer.poll(1.0)

    def produce(self, topic, key, value, **kwargs):
        self._result = self.__loop.create_future()
        super().produce(topic, key, value, **kwargs)

    def stop(self):
        self.__poll_thread.join()
        # super().stop()

    def acked(self, err, msg):
        if err:
            self.__loop.call_soon_threadsafe(self._result.set_exception, KafkaException(err))
        else:
            self.__loop.call_soon_threadsafe(self._result.set_result, msg)


if __name__ == '__main__':
    settings = AppSettings()
    topic = settings.tn_events

    kafka_settings = KafkaProducerCredentials(bootstrap_servers=settings.kafka_broker)
    kafka_settings.conf.update(
        {
            'client.id': socket.gethostname()
        }
    )
    producer = Producer(kafka_settings.conf)
    producer.produce(topic, key='key=sync_producer', value='hello world')

    async_producer = AsyncProducer(kafka_settings.conf)
    async_producer.start()

    for i in range(22):
        async_producer.produce(topic, key='key=async_producer', value=f'Hello world {i}')
        async_producer.stop()
