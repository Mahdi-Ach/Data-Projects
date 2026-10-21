import json
import os
import pandas as pd
from kafka import KafkaConsumer
from minio import Minio
from io import BytesIO

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
MINIO_SERVER = os.getenv('MINIO_SERVER', 'localhost:9002')

import time

consumer = KafkaConsumer(
    'events',
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    group_id=f'bronze-writer-{int(time.time())}',   # ← nouveau groupe à chaque run
    consumer_timeout_ms=10000,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

minio_client = Minio(MINIO_SERVER, access_key='minioadmin', secret_key='minioadmin123', secure=False)

if not minio_client.bucket_exists("bronze"):
    minio_client.make_bucket("bronze")
    print("Bucket 'bronze' créé")

events = []
for msg in consumer:
    events.append(msg.value)
    # if len(events) >= 10:
    #     break

if not events:
    print("Aucun message lu.")
else:
    df = pd.DataFrame(events)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    minio_client.put_object('bronze', 'events/part-0.parquet', parquet_buffer, length=len(parquet_buffer.getvalue()))
    print(f"Écrit {len(events)} événements dans bronze/events/part-0.parquet")