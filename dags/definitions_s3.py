from dagster import Definitions, asset, define_asset_job
import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from io import StringIO
from sqlalchemy import create_engine
from datetime import datetime, timedelta

S3_BUCKET = "npl-de18-lab8-data"
PG_URL = "postgresql://dagster:dagster@postgres:5432/dagster"

s3 = boto3.client('s3', endpoint_url='https://storage.yandexcloud.net', config=Config(signature_version=UNSIGNED))

def read_s3_jsonl(key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_json(StringIO(obj['Body'].read().decode('utf-8')), lines=True)

def get_yesterday():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

@asset
def raw_transactions_s3():
    key = f"day={get_yesterday()}/slot=09-30/transactions.jsonl"
    df = read_s3_jsonl(key)
    engine = create_engine(PG_URL)
    df.to_sql("raw_transactions_s3", engine, if_exists="replace", index=False)
    return df

@asset
def raw_cancellations_s3():
    key = f"cancellations/day={get_yesterday()}/cancellations.jsonl"
    df = read_s3_jsonl(key)
    engine = create_engine(PG_URL)
    df.to_sql("raw_cancellations_s3", engine, if_exists="replace", index=False)
    return df

@asset
def ref_users_s3():
    key = "reference/users.jsonl"
    df = read_s3_jsonl(key)
    engine = create_engine(PG_URL)
    df.to_sql("ref_users_s3", engine, if_exists="replace", index=False)
    return df

@asset
def ref_promo_codes_s3():
    key = "reference/promo_codes.jsonl"
    df = read_s3_jsonl(key)
    engine = create_engine(PG_URL)
    df.to_sql("ref_promo_codes_s3", engine, if_exists="replace", index=False)
    return df

@asset(deps=[raw_transactions_s3, ref_users_s3, ref_promo_codes_s3])
def fact_transactions_clean():
    engine = create_engine(PG_URL)
    transactions = pd.read_sql("SELECT * FROM raw_transactions_s3", engine)
    users = pd.read_sql("SELECT * FROM ref_users_s3", engine)
    promos = pd.read_sql("SELECT * FROM ref_promo_codes_s3", engine)
    
    # 1. Дедупликация transaction_id
    transactions = transactions.drop_duplicates(subset=['transaction_id'], keep='first')
    
    # 2. Пустой user_id → -1 (unknown)
    transactions['user_id'] = pd.to_numeric(transactions['user_id'], errors='coerce').fillna(-1).astype(int)
    
    # 3. Несуществующие user_id → пометить
    valid_users = set(users['user_id'].tolist())
    transactions['user_valid'] = transactions['user_id'].isin(valid_users)
    
    # 4. Нулевые суммы → пометить
    transactions['amount_zero'] = transactions['amount'] == 0
    
    # 5. Отрицательные суммы → оставить (не баг)
    
    # 6. Просроченные промокоды
    if 'promo_code_id' in transactions.columns and 'expires_at' in promos.columns:
        promos['expires_at'] = pd.to_datetime(promos['expires_at'], errors='coerce')
        transactions = transactions.merge(promos[['promo_code_id', 'expires_at']], on='promo_code_id', how='left')
        transactions['promo_expired'] = (transactions['expires_at'] < datetime.now()) & (transactions['promo_code_id'].notna())
    else:
        transactions['promo_expired'] = False
    
    # 7. Унификация дат
    transactions['created_at_dt'] = pd.to_datetime(transactions['created_at'], unit='s', errors='coerce')
    
    transactions.to_sql("fact_transactions_clean", engine, if_exists="replace", index=False)
    return transactions

@asset(deps=[raw_cancellations_s3])
def fact_cancellations_clean():
    engine = create_engine(PG_URL)
    cancellations = pd.read_sql("SELECT * FROM raw_cancellations_s3", engine)
    cancellations['cancelled_at_dt'] = pd.to_datetime(cancellations['cancelled_at'], errors='coerce')
    cancellations.to_sql("fact_cancellations_clean", engine, if_exists="replace", index=False)
    return cancellations

defs = Definitions(
    assets=[raw_transactions_s3, raw_cancellations_s3, ref_users_s3, ref_promo_codes_s3, 
            fact_transactions_clean, fact_cancellations_clean],
    jobs=[define_asset_job("ingest_s3_data")],
)

@asset(deps=[fact_transactions_clean])
def fact_hourly_stats():
    engine = create_engine(PG_URL)
    df = pd.read_sql("SELECT * FROM fact_transactions_clean", engine)
    
    df['hour'] = df['created_at_dt'].dt.hour
    hourly = df.groupby('hour').agg({
        'transaction_id': 'count',
        'amount': 'sum'
    }).reset_index()
    hourly.columns = ['hour', 'transaction_count', 'total_amount']
    
    hourly.to_sql("fact_hourly_stats", engine, if_exists="replace", index=False)
    return hourly

@asset(deps=[fact_transactions_clean, fact_cancellations_clean])
def fact_daily_revenue():
    engine = create_engine(PG_URL)
    df = pd.read_sql("SELECT * FROM fact_transactions_clean", engine)
    
    df['date'] = df['created_at_dt'].dt.date
    daily = df.groupby('date').agg({
        'transaction_id': 'count',
        'amount': 'sum'
    }).reset_index()
    daily.columns = ['date', 'transaction_count', 'total_amount']
    
    daily.to_sql("fact_daily_revenue", engine, if_exists="replace", index=False)
    return daily

@asset(deps=[fact_transactions_clean, ref_promo_codes_s3])
def fact_promo_analysis():
    engine = create_engine(PG_URL)
    transactions = pd.read_sql("SELECT * FROM fact_transactions_clean", engine)
    promos = pd.read_sql("SELECT * FROM ref_promo_codes_s3", engine)
    
    promo_stats = transactions[transactions['promo_code_id'].notna()].groupby('promo_code_id').agg({
        'transaction_id': 'count',
        'amount': 'sum'
    }).reset_index()
    promo_stats.columns = ['promo_code_id', 'usage_count', 'total_amount']
    
    promo_stats = promo_stats.merge(promos, on='promo_code_id', how='left')
    
    promo_stats.to_sql("fact_promo_analysis", engine, if_exists="replace", index=False)
    return promo_stats

defs = Definitions(
    assets=[raw_transactions_s3, raw_cancellations_s3, ref_users_s3, ref_promo_codes_s3, 
            fact_transactions_clean, fact_cancellations_clean,
            fact_hourly_stats, fact_daily_revenue, fact_promo_analysis],
    jobs=[define_asset_job("ingest_s3_data")],
)
