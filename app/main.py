import json
import socket
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Depends
from sse_starlette import EventSourceResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse, StreamingResponse

from app.workers.worker_input_output import create_database
from dependencies import get_producer, get_topic, get_cookies, get_headers
from event_bus import (
    Producer, Consumer, KafkaConsumerCredentials, KafkaProducerCredentials, TOPICS
)
from settings import AppSettings, LoggerSettings

logger = LoggerSettings().logger
SETTINGS = AppSettings()

# Open HTML
with open("client_sse.html", "r", encoding="utf-8") as html:
    HTML = html.read()

with open("client_sse_alert.html", "r", encoding="utf-8") as html:
    HTML_ALERT = html.read()


@asynccontextmanager
async def lifespan(app: FastAPI):
    topic = SETTINGS.topic

    kafka_settings = KafkaProducerCredentials(bootstrap_servers=SETTINGS.kafka_broker)
    kafka_settings.conf.update({'client.id': socket.gethostname()})
    kafka_settings.conf.update({'default.topic.config': {'produce.offset.report': True}})
    producer = Producer(kafka_settings.conf)
    app.state.topic = topic
    app.state.producer = producer

    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Transaction-Id"],
)


@app.middleware("http")
async def settings_middleware(request: Request, call_next):
    # request.state.consumer = request.app.state.consumer
    request.state.producer = request.app.state.producer
    request.state.topic = request.app.state.topic
    response = await call_next(request)
    return response


@app.get("/sse/watch")
async def get_html():
    return HTMLResponse(HTML)


@app.get("/sse/watch/alert")
async def get_alert():
    return HTMLResponse(HTML_ALERT)


@app.get("/sse/database/create/{account_id}")
def create(
    account_id: int,
    producer: Producer = Depends(get_producer),
):
    transaction_id = str(uuid.uuid1(account_id))
    topic = 'database_create'
    # Send to Kafka for worker to create database
    producer.produce(topic=topic, key=transaction_id, value='{"data": "creating database"}')
    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=SETTINGS.kafka_broker, group_id='42')
    max_offset = Consumer(kafka_settings.conf, topic).max_offset()
    return {'transaction_id': transaction_id, 'topic': topic, 'transaction_offset': max_offset}


# Use to:
# var source = new EventSource("http://localhost:8666/sse/subscribe/database_create/68fa0617-e49a-11ee-85d5-00000000000c/10086");
# source.onmessage = function(event) { console.log(event.data) };
@app.get("/sse/subscribe/{topic}/{transaction_id}/{transaction_offset}")
async def sse_stream(
    topic: str,
    transaction_id: uuid.UUID,
    transaction_offset: int = 0,
):
    logger.debug(f'Client {transaction_id=}')
    logger.debug(f'Client {topic=}')

    group_id = str(transaction_id)
    transaction_id = str(transaction_id)
    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=SETTINGS.kafka_broker, group_id=group_id)
    kafka_settings.conf.update({'enable.auto.commit': False})
    kafka_settings.conf.update({'auto.offset.reset': 'none'})
    kafka_settings.conf.update({'max.poll.interval.ms': '30'})
    consumer = Consumer(kafka_settings.conf, topic)
    return EventSourceResponse(subscribe_topic(consumer, transaction_id, transaction_offset))


@app.get("/sse/produce")
def produce(
    message: str = 'Hello World', count: int = 12, topic: str = Depends(get_topic),
    producer: Producer = Depends(get_producer)
):
    len_queue = 0
    for _ in range(count):
        len_queue = producer.produce(topic=topic, key="from fastapi", value=message)
    if len_queue == 0:
        return {'published': True}
    return {'published': False}


def subscribe_topic(consumer: Consumer, transaction_id: str = None, transaction_offset: int = 0):
    if transaction_id is None:
        transaction_id = str(uuid.uuid1())
    logger.debug(f'Transaction {transaction_id=}')

    max_offset = consumer.max_offset()
    reset = 0
    while True:
        message = consumer.poll(1.0)
        current_offset = consumer.current_offset()
        logger.debug(f'{current_offset=}')
        if reset == 0:
            consumer.reset_offset(transaction_offset)
            reset = 1

        if message is None:
            continue

        # input_value=(1, 1710706859694)
        timestamp = consumer.get_message_timestamp(message)
        message_key = message.key().decode('utf-8')
        if message_key != transaction_id:
            logger.debug(f'Key {message_key} != {transaction_id=}')
            continue
        else:
            logger.debug(f'Success {message_key} == {transaction_id=} !')
            consumer.commit(message)

        msg_error = message.error()
        if msg_error:
            logger.error(f"Consumer error: {msg_error}")
            continue

        responce = consumer.serialize_to_dict(message, transaction_id=transaction_id, timestamp=timestamp)
        data = {"data": responce}
        logger.debug(f'Responce {data=}')
        yield json.dumps(data)

    consumer.close()




@app.get("/sse/create_database")
def sse_create_database(
    producer: Producer = Depends(get_producer)
):
    topic = TOPICS.CREATING_DATABASE
    transaction_id = str(uuid.uuid1())
    logger.debug(f'Transaction {transaction_id=}')

    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=SETTINGS.kafka_broker, group_id=transaction_id)
    consumer = Consumer(kafka_settings.conf, topic)

    return EventSourceResponse(create_database(producer, consumer, transaction_id))


@app.get("/sse/stream")
async def stream(
    topic: str = Depends(get_topic),
    client: str = Depends(get_headers),
    cookies: str = Depends(get_cookies),
):
    logger.debug(f'Client is: {client=}')
    logger.debug(f'Client cookies is: {cookies=}')
    SETTINGS.group_id = str(uuid.uuid1())
    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=SETTINGS.kafka_broker, group_id=SETTINGS.group_id)
    consumer = Consumer(kafka_settings.conf, topic)
    listener_client = None
    logger.debug(f'Request {topic=}')
    return EventSourceResponse(subscribe_topic(consumer))


@app.get("/http/stream")
def http_stream(topic: str = Depends(get_topic)):
    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=SETTINGS.kafka_broker, group_id=SETTINGS.group_id)
    consumer = Consumer(kafka_settings.conf, topic)
    return StreamingResponse(subscribe_topic(consumer))


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8666, forwarded_allow_ips="*", proxy_headers=True)
    """
    Check:
    1. Press F12
    2. Open console
    3. Copipaste:
    var source = new EventSource("http://localhost:8666/sse/stream");
    source.onmessage = function get_mess(event) {
        console.log(event.data)    
    }
    """
