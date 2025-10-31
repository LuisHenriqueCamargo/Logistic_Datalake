# scripts/etl_silver_gold.py
import os
import pandas as pd
import s3fs
from sqlalchemy import create_engine, text
from s3_utils import ensure_bucket, BUCKET
from datetime import datetime

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "daxlog123")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "daxlog123")
POSTGRES_URI = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "postgresql+psycopg2://daxlog123:daxlog123@postgres:5432/airflow")

fs = s3fs.S3FileSystem(key=AWS_KEY, secret=AWS_SECRET, client_kwargs={'endpoint_url': S3_ENDPOINT})

def list_bronze_parts(prefix="bronze/telemetry/"):
    items = fs.glob(f"{BUCKET}/{prefix}**/data.parquet")
    keys = ["/".join(i.split("/")[1:]) for i in items]
    return keys

def read_parquet_key(key):
    path = f"s3://{BUCKET}/{key}"
    df = pd.read_parquet(path, engine='pyarrow', storage_options={'client_kwargs': {'endpoint_url': S3_ENDPOINT}, 'key': AWS_KEY, 'secret': AWS_SECRET})
    return df

def transform_and_enrich(df):
    df['speed_kmh'] = pd.to_numeric(df['speed_kmh'], errors='coerce').fillna(0)
    df['is_fast'] = df['speed_kmh'] > 80
    df['created_date'] = pd.to_datetime(df['created_at']).dt.date
    return df

def write_silver(df, prefix="silver/telemetry/"):
    if df.empty:
        return
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['year'] = df['created_at'].dt.year
    df['month'] = df['created_at'].dt.month
    df['day'] = df['created_at'].dt.day
    for (y,m,d), part in df.groupby(['year','month','day']):
        key = f"{prefix}year={y}/month={m:02d}/day={d:02d}/data.parquet"
        out_path = f"s3://{BUCKET}/{key}"
        part.to_parquet(out_path, engine='pyarrow', compression='snappy', index=False,
                        storage_options={'client_kwargs': {'endpoint_url': S3_ENDPOINT}, 'key': AWS_KEY, 'secret': AWS_SECRET})
        print("Wrote silver partition", out_path)

def write_gold_metrics(df):
    if df.empty:
        return
    df['created_date'] = pd.to_datetime(df['created_at']).dt.date
    metrics = df.groupby('created_date').agg(
        telemetry_count=pd.NamedAgg(column='id', aggfunc='count'),
        avg_speed=pd.NamedAgg(column='speed_kmh', aggfunc='mean'),
        pct_fast=pd.NamedAgg(column='is_fast', aggfunc=lambda x: x.mean())
    ).reset_index()
    engine = create_engine(POSTGRES_URI)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
    metrics.to_sql('telemetry_metrics', engine, schema='gold', if_exists='append', index=False, method='multi')
    print("Wrote gold metrics to Postgres (schema gold)")

def main():
    ensure_bucket()
    keys = list_bronze_parts()
    if not keys:
        print("No bronze partitions.")
        return
    dfs = []
    for key in keys:
        df = read_parquet_key(key)
        dfs.append(df)
    if not dfs:
        print("No data.")
        return
    full = pd.concat(dfs, ignore_index=True)
    enriched = transform_and_enrich(full)
    write_silver(enriched)
    write_gold_metrics(enriched)

if __name__ == "__main__":
    main()
