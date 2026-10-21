import os
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
from io import BytesIO

MINIO_SERVER = os.getenv('MINIO_SERVER', 'localhost:9002')
POSTGRES_SERVER = os.getenv('POSTGRES_SERVER', '127.0.0.1:5434')

minio_client = Minio(MINIO_SERVER, access_key='minioadmin', secret_key='minioadmin123', secure=False)
response = minio_client.get_object('silver', 'events/events_clean.parquet')
df = pd.read_parquet(BytesIO(response.read()))

engine = create_engine(f'postgresql+psycopg2://airflow:airflow@{POSTGRES_SERVER}/analytics')

# Supprimer la vue gold_events (dépend de events_clean)
with engine.begin() as conn:
    conn.execute(text("DROP VIEW IF EXISTS gold_events CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS events_clean CASCADE;"))

# Recréer la table avec la nouvelle structure (inclut user_id)
df.to_sql('events_clean', engine, if_exists='replace', index=False)

print(f"{len(df)} lignes chargées dans PostgreSQL")