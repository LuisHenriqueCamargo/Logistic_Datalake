# dags/silver_gold_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.sensors.external_task import ExternalTaskSensor # NOVO OPERADOR
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess
import os

default_args = {
    'owner': 'daxlog',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

def local_silver_gold(**kwargs):
    cmd = ["python", "/opt/airflow/scripts/etl_silver_gold.py"]
    subprocess.run(cmd, check=True)

# Databricks job payload example (adjust to your workspace) - Mantido
databricks_payload = {
  "new_cluster": {
    "spark_version": "12.1.x-scala2.12",
    "node_type_id": "Standard_D3_v2",
    "num_workers": 2
  },
  "spark_python_task": {
    "python_file": "dbfs:/FileStore/scripts/etl_silver_gold.py"
  }
}

with DAG(
    dag_id="silver_gold",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["silver","gold"]
) as dag:

    # === CORREÇÃO: SENSOR DE DEPENDÊNCIA EXTERNA ===
    # Garante que este DAG só inicie se o DAG bronze_processing tiver sucesso.
    wait_for_bronze_processing = ExternalTaskSensor(
        task_id="wait_for_bronze_processing",
        external_dag_id="bronze_processing",
        external_task_id="process_raw_to_bronze", # O ID da última tarefa do DAG BRONZE
        execution_delta=timedelta(seconds=0), 
        timeout=60 * 40, # 40 minutos de espera
        poke_interval=60,
        mode="poke"
    )

    t_local = PythonOperator(
        task_id="silver_gold_local",
        python_callable=local_silver_gold
    )

    t_databricks = DatabricksSubmitRunOperator(
        task_id="silver_gold_databricks",
        databricks_conn_id="databricks_default",
        json=databricks_payload,
        trigger_rule="none_failed_or_skipped"
    )

    # === ENCADENAMENTO ===
    # 1. Aguardar BRONZE
    # 2. Executar as tarefas Silver/Gold
    wait_for_bronze_processing >> t_local
    wait_for_bronze_processing >> t_databricks

    # O Airflow agora desenhará uma linha de dependência entre os 3 DAGs no seu Dashboard.