import socket
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.params import Depends
from sse_starlette import EventSourceResponse
from starlette.responses import HTMLResponse

from dependencies import get_consumer, get_producer, get_topic
from event_bus import Producer, Consumer, KafkaConsumerCredentials, KafkaProducerCredentials, AppSettings

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>SSE</title>
    </head>
    <body>
        <script>
            const evtSource = new EventSource("http://localhost:8666/sse/stream");
            evtSource.addEventListener("message", function(event) {
                // Logic to handle status updates
                console.log(event.data)
            });  
        </script>
    </body>
</html>
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
    consumer.subscribe(topic)
    consumer.poll(1.5)

    app.state.topic = topic
    app.state.producer = producer
    app.state.consumer = consumer

    yield
    app.state.producer.stop()
    app.state.consumer.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/sse/watch")
async def get():
    return HTMLResponse(html)


@app.get("/sse/produce")
def get(topic: str = Depends(get_topic), producer: Producer = Depends(get_producer)):
    len_queue = producer.produce(topic=topic, key="hello", value="world")
    if len_queue == 0:
        return {'published': True}
    return {'published': False}


def subscribe_topic(consumer: Consumer, topic: str):
    consumer.subscribe(topic)
    while True:
        messsage = consumer.poll(1.0)
        yield {"event": "message", "data": messsage}


@app.get("/sse/stream")
def stream(topic: str = Depends(get_topic), consumer: Consumer = Depends(get_consumer)):
    return EventSourceResponse(subscribe_topic(consumer, topic))


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8666)
