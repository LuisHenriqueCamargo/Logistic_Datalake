from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator 
# Importações Diretas (Corrigidas)
from etl_bronze import run_etl_bronze 
from etl_data_quality import run_soda_scan 

with DAG(
    dag_id="processamento_medallion_elt_senior",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["elt", "medallion", "senior_portfolio", "sql_pushdown", "data_quality"], 
) as dag:
    
    # 1. BRONZE: Extração/Carga Inicial (Python/Pandas)
    extracao_bronze = PythonOperator(
        task_id="ingestao_bronze",
        python_callable=run_etl_bronze, 
        execution_timeout=pendulum.duration(hours=1),
    )

    # 2. SILVER: Enriquecimento ELT (SQL Push-Down)
    enriquecimento_silver_elt = PostgresOperator(
        task_id='enriquecimento_silver_elt',
        postgres_conn_id='postgres_default', 
        sql='sql/silver_transform.sql',       
        autocommit=True, 
        execution_timeout=pendulum.duration(hours=1),
    )
    
    # 2.5. DATA QUALITY CHECK (Soda Core)
    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_soda_scan,
        op_kwargs={
            "table_name": "silver_enriched_awb_data",
            "checks_file": "silver_checks.yml", 
        },
        execution_timeout=pendulum.duration(hours=1),
    )
    
    # 3. GOLD: Modelagem ELT (SQL Push-Down)
    modelagem_gold_elt = PostgresOperator(
        task_id='modelagem_gold_elt',
        postgres_conn_id='postgres_default', 
        sql='sql/gold_transform.sql',         
        autocommit=True,
        execution_timeout=pendulum.duration(hours=1),
    )
    
    # Define o fluxo: Bronze -> Silver ELT -> Data Quality -> Gold ELT
    extracao_bronze >> enriquecimento_silver_elt >> data_quality_check >> modelagem_gold_elt