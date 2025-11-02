import logging
import json
import pandas as pd
from sqlalchemy import create_engine
import io 
from datetime import datetime
# REMOVIDO: import findspark (Não usa Spark apenas em cluster exemplo Databriks 

# Importações de funções utilitárias: s3_utils.py está em dags/
from s3_utils import BUCKET, get_minio_client, list_objects_in_raw, get_latest_object_content 

logger = logging.getLogger(__name__)

# =========================================================================
# CONFIGURAÇÕES DE CONEXÃO E TABELAS
# =========================================================================
POSTGRES_USER = 'daxlog123'
POSTGRES_PASSWORD = 'daxlog123'
POSTGRES_HOST = 'postgres' 
POSTGRES_PORT = 5432
POSTGRES_DB = 'airflow' 

BRONZE_TABLE_NAME = 'bronze_awb_data'
SCHEMA_NAME = 'public'

# Definindo os tipos de dados esperados para conversão no Pandas
DTYPE_MAP = {
    "event_id": str,
    "scan_timestamp": str, 
    "awb_number": str,
    "carrier_name": str,
    "sortation_line_id": str,
    "scanner_id": str,
    "product_category": str,
    "package_weight_kg": float, 
    "package_dimension_cm": str,
    "declared_value": float, 
    "scan_type": str,
    "scan_status": str,
    "origin_hub": str,
    "destination_hub": str,
    "process_time_sec": float,
    "is_exception": bool,
}


# =========================================================================
# FUNÇÃO DE CARGA MASSIVA (Plano Z original que funcionou)
# =========================================================================

def load_to_postgres_robust(df_pandas: pd.DataFrame, engine):
    """
    Carrega o DataFrame Pandas no PostgreSQL usando Psycopg2/COPY FROM.
    Usa TABLE TEMPORARY e NO QUOTING (correção) em uma única transação.
    """
    temp_table_name = 'temp_bronze_awb_staging'
    
    # 1. Configuração do buffer CSV
    csv_buffer = io.StringIO()
    # A correção crítica (quoting=None)
    df_pandas.to_csv(csv_buffer, index=False, header=False, sep='|', quoting=None, escapechar='\\', na_rep='') 
    csv_buffer.seek(0)
    
    raw_conn = None
    try:
        raw_conn = engine.raw_connection()
        
        with raw_conn.cursor() as cur:
            
            # 2. DROP e CRIAÇÃO da TABELA DE STAGING (TEMPORARY)
            logger.info("Iniciando Transação Única: Criação/Carga/Upsert.")
            cur.execute(f"""
                DROP TABLE IF EXISTS {SCHEMA_NAME}.{temp_table_name};
                
                CREATE TEMPORARY TABLE {temp_table_name} (
                    event_id VARCHAR,
                    scan_timestamp TIMESTAMP WITHOUT TIME ZONE,
                    awb_number VARCHAR,
                    carrier_name VARCHAR,
                    sortation_line_id VARCHAR,
                    scanner_id VARCHAR,
                    product_category VARCHAR,
                    package_weight_kg NUMERIC,
                    package_dimension_cm VARCHAR,
                    declared_value NUMERIC,
                    scan_type VARCHAR,
                    scan_status VARCHAR,
                    origin_hub VARCHAR,
                    destination_hub VARCHAR,
                    process_time_sec NUMERIC,
                    is_exception BOOLEAN
                ) ON COMMIT DROP; 
            """)
            logger.info(f"Tabela de staging TEMPORÁRIA criada no cursor atual.")
            
            # 3. CARGA MASSIVA (COPY FROM STDIN)
            logger.info(f"Iniciando carga massiva (COPY FROM STDIN) de {len(df_pandas)} registros...")
            cur.copy_from(
                csv_buffer, 
                f'{temp_table_name}', 
                sep='|', 
                columns=df_pandas.columns.tolist()
            )
            logger.info("Carga massiva para a tabela de staging concluída.")

            # 4. CRIAÇÃO da TABELA BRONZE (Se não existir e garante PK)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{BRONZE_TABLE_NAME} (
                    event_id VARCHAR PRIMARY KEY,
                    scan_timestamp TIMESTAMP WITHOUT TIME ZONE,
                    awb_number VARCHAR,
                    carrier_name VARCHAR,
                    sortation_line_id VARCHAR,
                    scanner_id VARCHAR,
                    product_category VARCHAR,
                    package_weight_kg NUMERIC,
                    package_dimension_cm VARCHAR,
                    declared_value NUMERIC,
                    scan_type VARCHAR,
                    scan_status VARCHAR,
                    origin_hub VARCHAR,
                    destination_hub VARCHAR,
                    process_time_sec NUMERIC,
                    is_exception BOOLEAN
                );
            """)
            logger.info(f"Tabela {BRONZE_TABLE_NAME} verificada/criada.")

            # 5. UPSERT (Carga Incremental)
            upsert_query = f"""
                INSERT INTO {SCHEMA_NAME}.{BRONZE_TABLE_NAME}
                SELECT * FROM {temp_table_name}
                ON CONFLICT (event_id) DO NOTHING;
            """
            cur.execute(upsert_query)
            logger.info("Transação UPSERT (ON CONFLICT DO NOTHING) concluída.")
            
            # 6. COMMIT FINAL
            raw_conn.commit() 
            logger.info("COMMIT final da transação. Carga Bronze concluída com sucesso.")

    except Exception as e:
        if raw_conn:
            raw_conn.rollback() 
        logger.error(f"Falha CRÍTICA durante a carga. ROLLBACK executado: {e}")
        raise
    finally:
        if raw_conn:
            raw_conn.close() 

            
# =========================================================================
# FUNÇÃO PRINCIPAL (run_etl_bronze() - PANDAS PURO)
# =========================================================================

def run_etl_bronze():
    
    logger.info("--- Iniciando ETL: RAW -> BRONZE (Pandas Puro + Psycopg2 Load) ---")
    
    # 1. Extração dos Dados do MinIO (S3)
    try:
        minio_client = get_minio_client()
        latest_key = list_objects_in_raw(minio_client, BUCKET, "raw/")
        
        if not latest_key:
            logger.warning("Nenhum arquivo encontrado na pasta RAW. ETL encerrado.")
            return

        file_content_bytes = get_latest_object_content(minio_client, BUCKET, latest_key)
        file_content_str = file_content_bytes.decode('utf-8')
        
        # Leitura de todo o arquivo na memória (funciona para 200 linhas)
        data = [json.loads(line) for line in file_content_str.splitlines()]
        df_pandas = pd.DataFrame(data)
        logger.info(f"Dados lidos do MinIO com sucesso. Total de linhas: {len(df_pandas)}")
        
    except Exception as e:
        logger.error(f"Erro durante a extração Pandas: {e}")
        raise

    # 2. Transformação com Pandas (Garantindo Tipos e Ordem)
    try:
        df_pandas = df_pandas.astype(DTYPE_MAP)
        
        df_pandas['scan_timestamp'] = pd.to_datetime(df_pandas['scan_timestamp'], errors='coerce')
        
        column_order = [
            "event_id", "scan_timestamp", "awb_number", "carrier_name", 
            "sortation_line_id", "scanner_id", "product_category", "package_weight_kg", 
            "package_dimension_cm", "declared_value", "scan_type", "scan_status", 
            "origin_hub", "destination_hub", "process_time_sec", "is_exception"
        ]
        
        df_pandas = df_pandas[column_order]
        
        logger.info("Transformação e garantia de tipos com Pandas concluída.")
        
    except Exception as e:
        logger.error(f"Erro durante a transformação Pandas: {e}")
        raise

    # 3. CARGA INCREMENTAL ROBUSTA
    try:
        db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(db_url)
        
        load_to_postgres_robust(df_pandas, engine)
        
        logger.info(f"Carga Incremental da Tabela Bronze concluída com sucesso. Verifique '{BRONZE_TABLE_NAME}' no PGAdmin.")

    except Exception as e:
        logger.error(f"Erro durante a carga incremental no PostgreSQL: {e}")
        raise
    
if __name__ == "__main__":
    run_etl_bronze()