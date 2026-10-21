import os
import pandas as pd
from minio import Minio
from io import BytesIO

MINIO_SERVER = os.getenv('MINIO_SERVER', 'localhost:9002')

minio_client = Minio(MINIO_SERVER, access_key='minioadmin', secret_key='minioadmin123', secure=False)

if not minio_client.bucket_exists("silver"):
    minio_client.make_bucket("silver")
    print("Bucket 'silver' créé")

response = minio_client.get_object('bronze', 'events/part-0.parquet')
df = pd.read_parquet(BytesIO(response.read()))
print(f"Lignes lues depuis bronze : {len(df)}")

df_clean = df.drop_duplicates(subset=['id'])
print(f"Lignes après dédoublonnage : {len(df_clean)}")

parquet_buffer = BytesIO()
df_clean.to_parquet(parquet_buffer, index=False)
parquet_buffer.seek(0)
minio_client.put_object('silver', 'events/events_clean.parquet', parquet_buffer, length=len(parquet_buffer.getvalue()))
print("Silver écrit avec succès")