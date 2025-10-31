import logging
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import io  # <-- NECESSÁRIO para a carga COPY FROM

# Importações de funções utilitárias: Importação direta, pois o s3_utils.py está em dags/
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

# =========================================================================
# FUNÇÃO DE CARGA MASSIVA (FINAL V4 - COM RESET DE CURSOR)
# =========================================================================

def load_to_postgres_incremental(df: pd.DataFrame, engine):
    """
    Carrega o DataFrame no PostgreSQL usando COPY FROM STDIN.
    Força o fechamento e reabertura do cursor entre a criação da tabela e o COPY FROM 
    para garantir a visibilidade da nova tabela no catálogo do Postgres.
    """
    temp_table_name = 'temp_bronze_awb_staging'
    
    # 1. Converte o DataFrame para um buffer CSV em memória (STDIN)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=False, sep='\t', quoting=1) 
    csv_buffer.seek(0)
    
    # 2. Executa todas as operações em uma ÚNICA raw_connection
    raw_conn = None
    try:
        raw_conn = engine.raw_connection()
        
        # --- BLOC A: CRIAÇÃO DA TABELA DE STAGING E COMMIT ---
        # Usa um cursor temporário para a criação e fecha-o
        with raw_conn.cursor() as cur:
            cur.execute(f"""
                DROP TABLE IF EXISTS {SCHEMA_NAME}.{temp_table_name};
                CREATE TABLE {SCHEMA_NAME}.{temp_table_name} (
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
            raw_conn.commit() # Commit para tornar a tabela visível
            logger.info(f"Tabela de staging {temp_table_name} criada e commitada.")
        
        # --- BLOC B: CARGA COPY FROM E UPSERT ---
        # Reabre o cursor para garantir que ele veja a nova tabela commitada
        with raw_conn.cursor() as cur:
            
            # Carga massiva (COPY FROM STDIN)
            logger.info(f"Iniciando carga massiva (COPY FROM STDIN) de {len(df)} registros...")
            cur.copy_from(
                csv_buffer, 
                f'{SCHEMA_NAME}.{temp_table_name}', 
                sep='\t', 
                columns=df.columns.tolist()
            )
            logger.info("Carga massiva para a tabela de staging concluída.")

            # Cria a tabela final (se não existir)
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

            # Executa o UPSERT (Carga Incremental)
            upsert_query = f"""
                INSERT INTO {SCHEMA_NAME}.{BRONZE_TABLE_NAME}
                SELECT * FROM {SCHEMA_NAME}.{temp_table_name}
                ON CONFLICT (event_id) DO NOTHING;
            """
            cur.execute(upsert_query)
            
            raw_conn.commit() # Commit final do UPSERT
            logger.info("Transação UPSERT concluída. Carga incremental concluída com sucesso.")

    except Exception as e:
        if raw_conn:
            raw_conn.rollback()
        logger.error(f"Falha CRÍTICA durante a carga: {e}")
        raise
    finally:
        if raw_conn:
            raw_conn.close() # FECHAMENTO GARANTIDO
            

def run_etl_bronze():
    # ... (O restante da função run_etl_bronze é o mesmo e não precisa ser modificado)
    """
    Função principal que orquestra a extração, transformação e a carga incremental.
    """
    logger.info("--- Iniciando ETL: RAW -> BRONZE (Incremental e Robusto) ---")
    
    # ... (EXTRAÇÃO e TRANSFORMAÇÃO)
    try:
        minio_client = get_minio_client()
        latest_key = list_objects_in_raw(minio_client, BUCKET, "raw/")
        if not latest_key:
            logger.warning("Nenhum arquivo encontrado na pasta RAW. ETL encerrado.")
            return

        file_content_bytes = get_latest_object_content(minio_client, BUCKET, latest_key)
        
        data = [json.loads(line) for line in file_content_bytes.decode('utf-8').splitlines()]
        df = pd.DataFrame(data)
        
        df['scan_timestamp'] = pd.to_datetime(df['scan_timestamp'])
        df['process_time_sec'] = df['process_time_sec'].astype(float)
        
    except Exception as e:
        logger.error(f"Erro durante a extração ou transformação: {e}")
        raise

    # 3. CARGA INCREMENTAL (Chamando a função de carga massiva)
    try:
        db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(db_url)
        
        load_to_postgres_incremental(df, engine)
        
        logger.info(f"Carga Incremental da Tabela Bronze concluída com sucesso. Verifique 'bronze_awb_data' no PGAdmin.")

    except Exception as e:
        logger.error(f"Erro durante a carga incremental no PostgreSQL: {e}")
        raise