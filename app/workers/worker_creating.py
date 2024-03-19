import json
from datetime import time

from event_bus import TOPICS
from settings import LoggerSettings
from worker_father import Worker

logger = LoggerSettings().logger


class WorkerCreating(Worker):

    def action(self):
        self.polling_message()

    # The strong operation
    def __create_database(self) -> bool:
        time.sleep(5)
        return True

    def polling_message(self):
        max_offset = self.consumer.max_offset()
        while True:
            message = self.consumer.poll(1.0)
            # current_offset = consumer.current_offset()
            # logger.debug(f'{current_offset=}')

            if message is None:
                continue

            msg_error = message.error()
            if msg_error:
                logger.error(f"Consumer error: {msg_error}")
                self.emit_error()
                continue

            # This is code place for creating database
            # Is bigger and more complex actions for production logic
            # but it's just example
            result = self.__create_database()
            if not result:
                self.emit_error()

            self.emit_success()
            timestamp = self.consumer.get_message_timestamp(message)
            responce = self.consumer.serialize_to_dict(message, transaction_id=self.transaction_id, timestamp=timestamp)
            data = {"data": responce}
            logger.debug(f'Responce {data=}')
            yield json.dumps(data)

        self.consumer.close()


if __name__ == '__main__':
    recived_topic = TOPICS.CREATED_DATABASE
    consumed_topic = TOPICS.CREATING_DATABASE
    error_topic = TOPICS.ERROR

    worker = WorkerCreating(recived_topic, consumed_topic, error_topic)
