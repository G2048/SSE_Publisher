import asyncio
from threading import Thread

import confluent_kafka as kafka

from event_bus.models import AppSettings, KafkaConsumerCredentials, LoggerSettings

logger = LoggerSettings().logger


class Consumer:

    def __init__(self, settings: dict):
        self.kafka_settings = settings
        self._command = ''
        self._cancelled = False
        self._action = None

    def add_settings(self, conf_key: str, value: str):
        self.kafka_settings.update({conf_key: value})

    def start(self):
        self.consumer = kafka.Consumer(self.kafka_settings)
        return self

    def subscribe(self, topic: str, on_assign=None, **kwargs):
        self._subscribe([topic], on_assign=on_assign, **kwargs)

    def list_subscribe(self, topics: list, on_assign=None, **kwargs):
        self._subscribe(topics, on_assign=on_assign, **kwargs)

    def _subscribe(self, topics: list, on_assign=None, **kwargs):
        if not on_assign:
            on_assign = self.reset_offset
        self.consumer.subscribe(topics, on_assign=on_assign, **kwargs)

    def stop(self):
        self.consumer.close()
        self._cancelled = True

    def poll(self, timeout):
        return self.consumer.poll(timeout=timeout)

    def poll_loop(self, timeout=1.0):
        if not self._action:
            raise RuntimeError('You must choose action for message!')
        try:
            # while not self._cancelled:
            yield_msg = self._loop(timeout)
            for msg in yield_msg:
                self._action(msg)
        except Exception:
            logger.warning('Error occurned!', exc_info=True)
        finally:
            # Leave group and commit final offsets
            self.stop()

    def _loop(self, timeout):
        logger.info('Start Loop...')
        while True:
            msg = self.poll(timeout)

            if msg is None:
                continue

            msg_error = msg.error()
            if msg_error:
                logger.error(f"Consumer error: {msg_error}")
                continue
            yield msg

    @staticmethod
    def check_action():
        pass

    @property
    def action(self):
        return self._action

    @action.setter
    def action(self, function):
        self._action = function

    @property
    def command(self):
        return self._command

    @command.setter
    def command(self, command):
        self._command = command

    def reset_offset(self, consumer, partitions):
        if self.command == 'reset':
            for partition in partitions:
                partition.offset = kafka.OFFSET_BEGINNING
            """Set the consumer partition assignment to the provided list of TopicPartition and start consuming."""
            consumer.assign(partitions)
            """Returns the current partition assignment."""
            # self.consumer.assignment()
            """Return this client’s broker-assigned group member id."""
            self.consumer.memberid()

def action(msg):
    logger.info(dir(msg))
    logger.info(msg.key())
    logger.info(msg.value())


class AsyncConsumer(Consumer):

    def start(self):
        self.__loop = asyncio.get_event_loop()
        self.__poll_thread = Thread(target=self.__poll_loop)
        self.__poll_thread.start()
        return super().start()

    def __poll_loop(self):
        while not self._cancelled:
            self.poll(1.0)

    def stop(self):
        self.__poll_thread.join()
        super().stop()


if __name__ == '__main__':
    settings = AppSettings()
    topic = settings.tn_events
    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=settings.kafka_broker, group_id=settings.group_id)

    consumer = Consumer(kafka_settings.conf)
    consumer.start()
    consumer.subscribe(topic)
    consumer.action = action
    consumer.poll_loop(1.5)
