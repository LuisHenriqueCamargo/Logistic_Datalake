# dags/dbt_core_only_test.py

from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# O caminho do projeto dbt dentro do container, mapeado no docker-compose
DBT_PROJECT_PATH = "/opt/airflow/dbt_project"

with DAG(
    dag_id="dbt_core_only_test",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dbt", "teste"],
    doc_md="DAG de teste isolado para validar a instalação e conexão do dbt Core.",
) as dag:
    
    start_task = BashOperator(
        task_id="log_start",
        bash_command="echo 'Iniciando teste de integração dbt Core...'"
    )

    # 1. TAREFA: Rodar o dbt Debug (Verifica a conexão com o DW)
    # Esta é a tarefa crítica de validação!
    dbt_debug_task = BashOperator(
        task_id="dbt_debug_connection",
        bash_command=f"dbt debug --project-dir {DBT_PROJECT_PATH}",
    )

    # 2. TAREFA: Rodar o dbt Run (Executa os modelos)
    dbt_run_task = BashOperator(
        task_id="dbt_run_all_models",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_PATH}",
    )
    
    # 3. TAREFA: Rodar o dbt Test (Verificar testes)
    dbt_test_task = BashOperator(
        task_id="dbt_run_all_tests",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_PATH}",
    )

    # Fluxo de Execução
    start_task >> dbt_debug_task >> dbt_run_task >> dbt_test_task