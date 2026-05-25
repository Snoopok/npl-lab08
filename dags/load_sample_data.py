from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import logging
import traceback

default_args = {
    'owner': 'anton_martemianov',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    dag_id='load_sample_data',
    default_args=default_args,
    description='Загрузка sample-данных в DuckDB',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

def load_data_to_duckdb():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    con = None
    try:
        con = duckdb.connect('/data/lab08.db')
        logger.info("✅ Соединение с DuckDB установлено")
        
        files = [
            ('raw_transactions', '/opt/airflow/samples/transactions_sample.jsonl'),
            ('raw_cancellations', '/opt/airflow/samples/cancellations_sample.jsonl'),
            ('raw_exchange_rates', '/opt/airflow/samples/exchange_rates_sample.jsonl'),
            ('ref_users', '/opt/airflow/samples/users.jsonl'),
            ('ref_promo_codes', '/opt/airflow/samples/promo_codes.jsonl'),
        ]
        
        for table, path in files:
            logger.info(f"→ Загружаю {table} из {path}...")
            try:
                con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_json_auto('{path}');")
                logger.info(f"✅ {table} загружена")
            except Exception as e:
                logger.error(f"❌ Ошибка {table}: {e}")
                logger.error(traceback.format_exc())
        
        logger.info("✅ Все данные загружены")
        logger.info(str(con.execute("SHOW TABLES").df()))
    except Exception as e:
        logger.error(f"❌ Общая ошибка: {e}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if con:
            con.close()
            logger.info("✅ Соединение закрыто")

load_task = PythonOperator(
    task_id='load_sample_data',
    python_callable=load_data_to_duckdb,
    dag=dag,
)
