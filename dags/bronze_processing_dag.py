import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

# 🚨 Importação direta do arquivo movido para a pasta dags/
# O Airflow garante que o conteúdo de dags/ esteja no PYTHONPATH do Worker
from etl_bronze import run_etl_bronze 

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'daxlog',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="processamento_de_bronze",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "etl", "incremental", "teste_isolado"]
) as dag:

    process_raw_to_bronze = PythonOperator(
        task_id="processar_bronze_incremental",
        python_callable=run_etl_bronze # Chamada direta
    )