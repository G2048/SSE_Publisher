import json

from app.workers.worker_father import Worker
from event_bus import (
    TOPICS,
)
from settings import LoggerSettings

logger = LoggerSettings().logger


class WorkerInputOutput(Worker):

    def action(self):
        self.polling_message()

    def polling_message(self):
        while True:
            message = self.consumer.poll(1.0)

            if message is None:
                continue

            msg_error = message.error()
            if msg_error:
                logger.error(f"Consumer error: {msg_error}")
                self.error()
                continue

            self.success()
            timestamp = self.consumer.get_message_timestamp(message)
            responce = self.consumer.serialize_to_dict(message, transaction_id=self.transaction_id, timestamp=timestamp)
            data = {"data": responce}
            logger.debug(f'Responce {data=}')
            yield json.dumps(data)

        self.consumer.close()


if __name__ == '__main__':
    recived_topic = TOPICS.CREATING_DATABASE
    consumed_topic = TOPICS.CREATED_DATABASE
    error_topic = TOPICS.ERROR

    worker = WorkerInputOutput(recived_topic, consumed_topic, error_topic)
