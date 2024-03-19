from abc import ABC, abstractmethod
from uuid import uuid1

from app.main import SETTINGS
from event_bus import (
    Producer, Consumer, EventModel, KafkaConsumerCredentials, KafkaProducerCredentials,
)
from settings import LoggerSettings

logger = LoggerSettings().logger


class Worker(ABC):
    """Every worker must be created for strictly execution the specify a buisnes logic"""

    def __init__(self, input_topic: str, output_topic: str, error_topic: str):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.error_topic = error_topic
        self.transation_id = uuid1()
        self.__setup_producer()

    def __setup_producer(self):
        kafka_settings = KafkaProducerCredentials(bootstrap_servers=SETTINGS.kafka_broker)
        kafka_settings.conf.update({'default.topic.config': {'produce.offset.report': True}})
        self.producer = Producer(kafka_settings.conf)

    def __setup_consumer(self):
        kafka_settings = KafkaConsumerCredentials(bootstrap_servers=SETTINGS.kafka_broker, group_id=self.transaction_id)
        self.consumer = Consumer(kafka_settings.conf, self.output_topic)

    def __setup_error_consumer(self):
        kafka_settings = KafkaConsumerCredentials(bootstrap_servers=SETTINGS.kafka_broker, group_id=self.transaction_id)
        self.consumer = Consumer(kafka_settings.conf, self.output_topic)

    # subscribe to topic for created database
    def subscribe(self):
        self.__setup_consumer(self.transaction_id)

    # Если действие (.action) успешно, то тогда публикуем событие
    def publish(self):
        last_offset = self.consumer.max_offset()
        # Prepare event for create database
        event_data = EventModel(
            key=self.transaction_id,
            message=self.input_topic,
            transaction_id=self.transaction_id,
            last_offset=last_offset
        )
        # Send event for create database
        self.producer.produce(topic=self.input_topic, key=self.transation_id, value=event_data.json())

    @abstractmethod
    def action(self):
        """ action for specify a buisnes logic """
        pass
