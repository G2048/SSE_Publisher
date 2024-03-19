import json

from event_bus import (
    TOPICS,
)
from settings import LoggerSettings

logger = LoggerSettings().logger


class WorkerInputOutput:

    def action(self):
        self.polling_message()

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
                continue

            # message_key = message.key().decode('utf-8')
            # if message_key != transaction_id:
            #     logger.debug(f'Key {message_key} != {transaction_id=}')
            #     continue
            # else:
            #     logger.debug(f'Success {message_key} == {transaction_id=} !')
            #     consumer.commit(message)

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
