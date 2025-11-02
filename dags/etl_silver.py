import logging
import pandas as pd
from sqlalchemy import create_engine
import io 
from datetime import datetime

logger = logging.getLogger(__name__)

# =========================================================================
# CONFIGURAÇÕES
# =========================================================================
POSTGRES_USER = 'daxlog123'
POSTGRES_PASSWORD = 'daxlog123'
POSTGRES_HOST = 'postgres' 
POSTGRES_PORT = 5432
POSTGRES_DB = 'airflow' 

BRONZE_TABLE_NAME = 'bronze_awb_data'
SILVER_TABLE_NAME = 'silver_enriched_awb_data'
SCHEMA_NAME = 'public'

# =========================================================================
# FUNÇÃO DE CARGA MASSIVA (Reutiliza a lógica robusta de COPY FROM)
# =========================================================================

def load_to_postgres_silver(df_pandas: pd.DataFrame, engine):
    """
    Carrega o DataFrame Pandas no PostgreSQL Silver, garantindo a DDL para o GOLD.
    """
    temp_table_name = 'temp_silver_staging'
    
    csv_buffer = io.StringIO()
    df_pandas.to_csv(csv_buffer, index=False, header=False, sep='|', quoting=None, escapechar='\\', na_rep='') 
    csv_buffer.seek(0)
    
    raw_conn = None
    try:
        raw_conn = engine.raw_connection()
        
        with raw_conn.cursor() as cur:
            
            # 1. DROP (Limpeza) E CRIAÇÃO da TABELA SILVER (Definitiva) - GARANTINDO A NOVA COLUNA
            cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{SILVER_TABLE_NAME} CASCADE;")
            cur.execute(f"""
                CREATE TABLE {SCHEMA_NAME}.{SILVER_TABLE_NAME} (
                    event_id VARCHAR PRIMARY KEY,
                    scan_timestamp TIMESTAMP WITHOUT TIME ZONE,
                    awb_number VARCHAR,
                    carrier_name VARCHAR,
                    product_category VARCHAR,
                    package_weight_kg NUMERIC,
                    declared_value NUMERIC,
                    is_exception BOOLEAN,
                    is_high_value BOOLEAN,
                    destination_hub VARCHAR,
                    process_time_sec NUMERIC NOT NULL -- <--- NOVO CAMPO GARANTIDO
                );
            """)
            logger.info(f"Tabela Silver '{SILVER_TABLE_NAME}' RE-CRIADA com sucesso.")
            
            # 2. CRIAÇÃO da TABELA DE STAGING (TEMPORARY)
            cur.execute(f"""
                DROP TABLE IF EXISTS {temp_table_name};
                CREATE TEMPORARY TABLE {temp_table_name} (
                    event_id VARCHAR,
                    scan_timestamp TIMESTAMP WITHOUT TIME ZONE,
                    awb_number VARCHAR,
                    carrier_name VARCHAR,
                    product_category VARCHAR,
                    package_weight_kg NUMERIC,
                    declared_value NUMERIC,
                    is_exception BOOLEAN,
                    is_high_value BOOLEAN,
                    destination_hub VARCHAR,
                    process_time_sec NUMERIC
                ) ON COMMIT DROP; 
            """)
            
            # 3. CARGA MASSIVA (COPY FROM STDIN)
            cur.copy_from(
                csv_buffer, 
                f'{temp_table_name}', 
                sep='|', 
                columns=df_pandas.columns.tolist()
            )

            # 4. INSERT (Full Reload - Silver)
            cur.execute(f"""
                INSERT INTO {SCHEMA_NAME}.{SILVER_TABLE_NAME}
                SELECT * FROM {temp_table_name};
            """)
            
            raw_conn.commit() 
            logger.info("Carga Silver concluída com sucesso.")

    except Exception as e:
        if raw_conn:
            raw_conn.rollback() 
        logger.error(f"Falha CRÍTICA durante a carga Silver. ROLLBACK executado: {e}")
        raise
    finally:
        if raw_conn:
            raw_conn.close() 

            
def run_etl_silver():
    
    logger.info("--- Iniciando ETL: BRONZE -> SILVER (Pandas Puro) ---")
    
    db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    engine = create_engine(db_url)
    
    # 1. EXTRAÇÃO (Leitura total da tabela BRONZE)
    try:
        sql_query = f"SELECT * FROM {SCHEMA_NAME}.{BRONZE_TABLE_NAME}"
        
        with engine.connect() as conn: 
            df_bronze = pd.read_sql(sql_query, conn.connection) 
        
        if df_bronze.empty:
            logger.warning(f"A tabela Bronze está vazia. ETL Silver encerrada.")
            return

        logger.info(f"Dados lidos do Bronze com sucesso. Total de linhas: {len(df_bronze)}")
        
    except Exception as e:
        logger.error(f"Erro durante a leitura da tabela Bronze: {e}")
        raise

    # 2. TRANSFORMAÇÃO (Enriquecimento, Cálculo de Tempo e Seleção)
    try:
        # CONVERSÃO: Garante que o timestamp seja datetime para cálculo
        df_bronze['scan_timestamp'] = pd.to_datetime(df_bronze['scan_timestamp'])

        # Enriquecimento 1: Cria a coluna de alto valor (> 1000)
        df_bronze['is_high_value'] = df_bronze['declared_value'].fillna(0) > 1000
        
        # Enriquecimento 2: CÁLCULO DE TEMPO DE PROCESSAMENTO (CORRIGIDO)
        # -------------------------------------------------------------------------
        # 1. Calcula o delta de tempo (máx - min) para CADA AWB (por awb_number)
        time_diff = df_bronze.groupby('awb_number')['scan_timestamp'].agg(['min', 'max'])
        df_bronze['process_time_sec'] = (time_diff['max'] - time_diff['min']).dt.total_seconds().reindex(df_bronze['awb_number']).values
        # 2. A reindexação garante que o valor do tempo total seja mapeado para cada linha correspondente ao AWB.
        # -------------------------------------------------------------------------
        
        # Ajusta NaN's em process_time_sec (AWBs com 1 scan, onde min=max) para 0
        df_bronze['process_time_sec'] = df_bronze['process_time_sec'].fillna(0)


        # Seleção de Colunas para o Silver - AGORA INCLUI process_time_sec (Passo 3 do T)
        df_silver = df_bronze[[
            "event_id", "scan_timestamp", "awb_number", "carrier_name", 
            "product_category", "package_weight_kg", "declared_value", 
            "is_exception", "is_high_value", "destination_hub", 
            "process_time_sec" # <-- AQUI A COLUNA DEVE EXISTIR!
        ]].copy()

        logger.info(f"Enriquecimento SILVER com 'process_time_sec' concluído.")
        
    except Exception as e:
        logger.error(f"Erro durante a transformação/enriquecimento Silver: {e}")
        raise

    # 3. CARGA SILVER
    try:
        load_to_postgres_silver(df_silver, engine)
        logger.info(f"Carga da Tabela Silver concluída com sucesso.")

    except Exception as e:
        logger.error(f"Erro durante a carga Silver no PostgreSQL: {e}")
        raise
    
if __name__ == "__main__":
    run_etl_silver()