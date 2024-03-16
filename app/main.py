import socket
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, Depends
from sse_starlette import EventSourceResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse, StreamingResponse

from dependencies import get_producer, get_topic
from event_bus import (
    Producer, Consumer, KafkaConsumerCredentials, KafkaProducerCredentials, AppSettings,
    LoggerSettings,
)

logger = LoggerSettings().logger
SETTINGS = AppSettings()

# Open HTML
with open("client_sse.html", "r", encoding="utf-8") as html:
    HTML = html.read()


@asynccontextmanager
async def lifespan(app: FastAPI):
    topic = SETTINGS.tn_events

    kafka_settings = KafkaProducerCredentials(bootstrap_servers=SETTINGS.kafka_broker)
    kafka_settings.conf.update({'client.id': socket.gethostname()})
    producer = Producer(kafka_settings.conf)
    app.state.topic = topic
    app.state.producer = producer

    yield
    app.state.producer.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    # request.state.consumer = request.app.state.consumer
    request.state.producer = request.app.state.producer
    request.state.topic = request.app.state.topic
    response = await call_next(request)
    return response


@app.get("/sse/watch")
async def get():
    return HTMLResponse(HTML)


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


def subscribe_topic(consumer: Consumer, topic: str):
    consumer.subscribe(topic)
    logger.debug(f'Subscribed to {topic=}!')
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue

        msg_error = message.error()
        if msg_error:
            logger.error(f"Consumer error: {msg_error}")
            continue

        time.sleep(0.5)
        responce = message.key().decode('utf-8') + ': ' + message.value().decode('utf-8')
        data = {"data": responce}
        logger.debug(f'Responce {data=}')
        yield data
    consumer.close()


@app.get("/sse/stream")
def stream(topic: str = Depends(get_topic)):
    SETTINGS.group_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=SETTINGS.kafka_broker, group_id=SETTINGS.group_id)
    consumer = Consumer(kafka_settings.conf)

    logger.debug(f'Request {topic=}')
    return EventSourceResponse(subscribe_topic(consumer, topic))


@app.get("/http/stream")
def http_stream(topic: str = Depends(get_topic)):
    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=SETTINGS.kafka_broker, group_id=SETTINGS.group_id)
    consumer = Consumer(kafka_settings.conf)
    return StreamingResponse(subscribe_topic(consumer, topic))


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8666)
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
