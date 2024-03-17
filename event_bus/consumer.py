import asyncio
from datetime import datetime
from threading import Thread

import confluent_kafka as kafka

from event_bus.models import KafkaConsumerCredentials, EventModel
from settings import AppSettings, LoggerSettings

logger = LoggerSettings().logger


class Consumer:
    def __init__(self, settings: dict, topic: str, partition: int = 0):
        self.kafka_settings = settings
        self.topic = topic
        self.partition = partition
        self.topic_partition = kafka.TopicPartition
        self.consumer = kafka.Consumer(self.kafka_settings)
        self.on_assign = None
        self._action = None

        self._subscribe([self.topic], self.partition, on_assign=self.on_assign)

    def _subscribe(self, topics: list, partition: int = 0, on_assign=None, **kwargs) -> list:
        if on_assign is None:
            on_assign = self.reset_offset_command
        logger.debug(f'Subscribed to {topics=}!')
        logger.debug(f'{on_assign=}')
        for topic in topics:
            self.consumer.assign([self.topic_partition(topic, partition=self.partition)])
        # self.consumer.subscribe(topics, on_assign=on_assign, **kwargs)
        # self.reset_offset(topic, partition=partition)
        return self.consumer.assignment()

    def commit(self, message=None):
        self.consumer.commit(message)

    def reset_offset(self, offset=kafka.OFFSET_BEGINNING):
        self.consumer.seek(kafka.TopicPartition(self.topic, partition=self.partition, offset=offset))

    def min_offset(self):
        return self.__get_offsets()[0]

    def max_offset(self):
        return self.__get_offsets()[1] - 1

    # Возвращает минимальный и последний+1 оффсеты
    def __get_offsets(self):
        return self.consumer.get_watermark_offsets(self.topic_partition(self.topic, partition=self.partition))

    def current_offset(self):
        return self.__get_current_offset().offset - 1

    def __get_current_offset(self):
        return self.consumer.position([self.topic_partition(self.topic, partition=self.partition)])[0]

    def close(self):
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
            self.close()

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

    def reset_offset_command(self, consumer, partitions):
        if self.command == 'reset':
            for partition in partitions:
                partition.offset = kafka.OFFSET_BEGINNING
                logger.debug(f'Reset offset {partition=}')
            """Set the consumer partition assignment to the provided list of TopicPartition and start consuming."""
            print(consumer.assign(partitions))
            """Returns the current partition assignment."""
            # self.consumer.assignment()
            """Return this client’s broker-assigned group member id."""
            # self.consumer.memberid()

    @staticmethod
    def serialize_to_event_model(msg, **kwargs):
        return EventModel(
            key=msg.key().decode('utf-8'),
            message=msg.value().decode('utf-8'),
            **kwargs
        )

    @staticmethod
    def serialize_to_dict(msg, **kwargs):
        return EventModel(
            key=msg.key().decode('utf-8'),
            message=msg.value().decode('utf-8'),
            **kwargs
        ).model_dump()

    @staticmethod
    def message_to_timestamp(msg):
        return datetime.fromtimestamp(msg.timestamp()[1] / 1000).isoformat()


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

    def close(self):
        self.__poll_thread.join()
        super().close()


if __name__ == '__main__':
    settings = AppSettings()
    topic = 'database_create'
    settings.group_id = '17'
    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=settings.kafka_broker, group_id=settings.group_id)
    kafka_settings.conf.update(
        {
            'auto.offset.reset': 'earliest',
        }
    )

    logger.debug(kafka_settings.conf)
    consumer = Consumer(kafka_settings.conf, topic)
    # consumer.command = 'reset'
    # consumer.action = action
    # consumer.poll_loop(1.5)
    max_offset = consumer.max_offset()

    count = 0
    flag = 0
    while True:
        msg = consumer.poll(1.5)
        offset = consumer.current_offset()
        logger.debug(offset)

        if flag == 0:
            # То есть мы начинаем читать с выбранного оффсета
            consumer.reset_offset(max_offset - 3)
            logger.debug('Reset!')
            flag = 1

        if msg is None:
            continue

        msg_error = msg.error()
        if msg_error:
            logger.error(f"Consumer error: {msg_error}")
            continue
        consumer.commit(msg)
        count += 1
        # logger.debug(msg.value())
        logger.debug(f'{count} - {msg.key().decode("utf-8")}')
