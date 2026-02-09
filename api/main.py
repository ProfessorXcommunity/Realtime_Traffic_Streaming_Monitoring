from fastapi import FastAPI 
from kafka import KafkaProducer 
import json 
import uuid ,time
from datetime import datetime

app = FastAPI()

producer = None

def get_producer():
    global producer 
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8")

            )
            print("Kafka producer connected")
        except Exception as e:
            print("waiting for kafka...",e)
            time.sleep(5)

get_producer()

@app.post("/event")
def ingest_event(event: dict):
    event['event_id'] = str(uuid.uuid4())
    event['timestamp'] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    producer.send('web_events',event)
    return {"status" : "ok"}