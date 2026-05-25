from dagster import Definitions, asset, define_asset_job
from dagster_postgres import DagsterPostgresStorage
import pandas as pd
from sqlalchemy import create_engine

PG_URL = "postgresql://dagster:dagster@postgres:5432/dagster"

@asset
def raw_transactions():
    df = pd.read_json('/opt/dagster/dags/samples/transactions_sample.jsonl', lines=True)
    engine = create_engine(PG_URL)
    df.to_sql("raw_transactions", engine, if_exists="replace", index=False)
    return df

@asset
def raw_cancellations():
    df = pd.read_json('/opt/dagster/dags/samples/cancellations_sample.jsonl', lines=True)
    engine = create_engine(PG_URL)
    df.to_sql("raw_cancellations", engine, if_exists="replace", index=False)
    return df

@asset
def ref_users():
    df = pd.read_json('/opt/dagster/dags/samples/users.jsonl', lines=True)
    engine = create_engine(PG_URL)
    df.to_sql("ref_users", engine, if_exists="replace", index=False)
    return df

defs = Definitions(
    assets=[raw_transactions, raw_cancellations, ref_users],
    jobs=[define_asset_job("load_sample_data")],
)
