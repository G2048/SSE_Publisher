import socket
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Depends
from sse_starlette import EventSourceResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse, StreamingResponse

from dependencies import get_consumer, get_producer, get_topic
from event_bus import (
    Producer, Consumer, KafkaConsumerCredentials, KafkaProducerCredentials, AppSettings,
    LoggerSettings,
)

logger = LoggerSettings().logger

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>SSE</title>
    </head>
    <body>
        <h1>Server Response:</h1>
        <div id="kafka_event">
        <script>
            var source = new EventSource("http://localhost:8666/sse/stream");
            source.onmessage = function(event) {
                document.getElementById("kafka_event").innerHTML += event.key + ": " + event.value + "<br>";
            };
        </script>
    </body>
</html>
"""
"""
            evtSource.addEventListener("data", function(event) {
                // Logic to handle status updates
                console.log(event.data)
            });  
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = AppSettings()
    topic = settings.tn_events

    kafka_settings = KafkaProducerCredentials(bootstrap_servers=settings.kafka_broker)
    producer = Producer(kafka_settings.conf)
    producer.add_settings('client.id', socket.gethostname())
    producer.start()

    kafka_settings = KafkaConsumerCredentials(bootstrap_servers=settings.kafka_broker, group_id=settings.group_id)
    consumer = Consumer(kafka_settings.conf)
    consumer.start()

    app.state.topic = topic
    app.state.producer = producer
    app.state.consumer = consumer

    yield
    app.state.producer.stop()
    app.state.consumer.stop()


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
    request.state.consumer = request.app.state.consumer
    request.state.producer = request.app.state.producer
    request.state.topic = request.app.state.topic
    response = await call_next(request)
    return response


@app.get("/sse/watch")
async def get():
    return HTMLResponse(html)


@app.get("/sse/produce")
def produce(message: str = None, topic: str = Depends(get_topic), producer: Producer = Depends(get_producer)):
    if not message:
        message = 'Hello World!'
    for _ in range(22):
        len_queue = producer.produce(topic=topic, key="from fastapi", value=message)
    if len_queue == 0:
        return {'published': True}
    return {'published': False}


def subscribe_topic(consumer: Consumer, topic: str):
    consumer.subscribe(topic)
    logger.debug(f'Subscribed to {topic=}!')
    while True:
        message = consumer.poll(1.0)
        # message = 'Test message'
        if message is None:
            continue

        msg_error = message.error()
        if msg_error:
            logger.error(f"Consumer error: {msg_error}")
            continue

        time.sleep(0.5)
        responce = message.key().decode('utf-8') + ': ' + message.value().decode('utf-8')
        data = {"data": responce, "key": message.key().decode('utf-8'), "value": message.value().decode('utf-8')}
        logger.debug(f'Responce {data=}')
        yield data


@app.get("/sse/stream")
def stream(topic: str = Depends(get_topic), consumer: Consumer = Depends(get_consumer)):
    # settings = AppSettings()
    # settings.group_id = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    logger.debug(f'Request {topic=}')
    return EventSourceResponse(subscribe_topic(consumer, topic))


@app.get("/http/stream")
def http_stream(topic: str = Depends(get_topic), consumer: Consumer = Depends(get_consumer)):
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
