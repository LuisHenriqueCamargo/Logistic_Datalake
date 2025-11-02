import logging
import pandas as pd
from sqlalchemy import create_engine
import io 
from datetime import datetime

logger = logging.getLogger(__name__)

# =========================================================================
# CONFIGURAÇÕES DE CONEXÃO E TABELAS
# =========================================================================
POSTGRES_USER = 'daxlog123'
POSTGRES_PASSWORD = 'daxlog123'
POSTGRES_HOST = 'postgres' 
POSTGRES_PORT = 5432
POSTGRES_DB = 'airflow' 

SILVER_TABLE_NAME = 'silver_enriched_awb_data' 
GOLD_TABLE_NAME = 'gold_volumes_by_destination'
SCHEMA_NAME = 'public'

# =========================================================================
# FUNÇÕES DE CARGA E MODELAGEM
# =========================================================================

def load_to_postgres_gold(df_pandas: pd.DataFrame, engine):
    """
    Carrega o DataFrame Pandas no PostgreSQL Gold usando Psycopg2/COPY FROM
    e executa a estratégia de Truncate/Insert (reconstrução total para Gold).
    """
    temp_table_name = 'temp_gold_staging'
    
    csv_buffer = io.StringIO()
    df_pandas.to_csv(csv_buffer, index=False, header=False, sep='|', quoting=None, escapechar='\\', na_rep='') 
    csv_buffer.seek(0)
    
    raw_conn = None
    try:
        raw_conn = engine.raw_connection()
        
        with raw_conn.cursor() as cur:
            
            # 1. CRIAÇÃO da TABELA GOLD (Definitiva)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{GOLD_TABLE_NAME} (
                    destination_hub VARCHAR PRIMARY KEY,
                    total_packages NUMERIC,
                    total_weight_kg NUMERIC,
                    avg_process_time_sec NUMERIC,
                    total_high_value_packages NUMERIC, 
                    last_processed_at TIMESTAMP WITHOUT TIME ZONE
                );
            """)
            logger.info(f"Tabela Gold '{GOLD_TABLE_NAME}' verificada/criada.")
            
            # 2. CRIAÇÃO da TABELA DE STAGING (TEMPORARY)
            cur.execute(f"""
                DROP TABLE IF EXISTS {temp_table_name};
                CREATE TEMPORARY TABLE {temp_table_name} (
                    destination_hub VARCHAR,
                    total_packages NUMERIC,
                    total_weight_kg NUMERIC,
                    avg_process_time_sec NUMERIC,
                    total_high_value_packages NUMERIC,
                    last_processed_at TIMESTAMP WITHOUT TIME ZONE
                ) ON COMMIT DROP; 
            """)
            
            # 3. CARGA MASSIVA (COPY FROM STDIN)
            cur.copy_from(
                csv_buffer, 
                f'{temp_table_name}', 
                sep='|', 
                columns=df_pandas.columns.tolist()
            )

            # 4. ESTRATÉGIA GOLD: TRUNCATE/INSERT (Reconstrução Total)
            cur.execute(f"""
                TRUNCATE TABLE {SCHEMA_NAME}.{GOLD_TABLE_NAME};
                INSERT INTO {SCHEMA_NAME}.{GOLD_TABLE_NAME}
                SELECT * FROM {temp_table_name};
            """)
            
            # 5. COMMIT FINAL
            raw_conn.commit() 
            logger.info("COMMIT final da transação. Carga Gold concluída com sucesso.")

    except Exception as e:
        if raw_conn:
            raw_conn.rollback() 
        logger.error(f"Falha CRÍTICA durante a carga Gold. ROLLBACK executado: {e}")
        raise
    finally:
        if raw_conn:
            raw_conn.close() 

            
def run_etl_gold():
    
    logger.info("--- Iniciando ETL: SILVER -> GOLD (Pandas Puro) ---")
    
    db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    engine = create_engine(db_url)
    
    # 1. EXTRAÇÃO (Leitura total da tabela SILVER)
    try:
        sql_query = f"SELECT * FROM {SCHEMA_NAME}.{SILVER_TABLE_NAME}"
        
        # --------------------------- CORREÇÃO APLICADA (CONEXÃO) ---------------------------
        # Abre a conexão SQLAlchemy, mas passa o objeto de CONEXÃO BRUTA (.connection)
        with engine.connect() as conn: 
            df_silver = pd.read_sql(sql_query, conn.connection) # <-- CORREÇÃO
        # ---------------------------------------------------------------------------------
        
        if df_silver.empty:
            logger.warning(f"A tabela Silver está vazia. ETL Gold encerrado.")
            return

        logger.info(f"Dados lidos do Silver com sucesso. Total de linhas: {len(df_silver)}")
        
    except Exception as e:
        logger.error(f"Erro durante a leitura da tabela Silver: {e}")
        raise

    # 2. TRANSFORMAÇÃO (Modelagem de Dados)
    try:
        # A coluna 'process_time_sec' agora existe graças ao ajuste no Silver!
        df_gold = (
            df_silver.groupby('destination_hub')
            .agg(
                total_packages=('event_id', 'count'),
                total_weight_kg=('package_weight_kg', 'sum'),
                avg_process_time_sec=('process_time_sec', 'mean'),
                total_high_value_packages=('is_high_value', 'sum') 
            )
            .reset_index()
        )
        
        df_gold['last_processed_at'] = datetime.now()
        
        # Garante a ordem das colunas de destino
        df_gold = df_gold[[
            "destination_hub", "total_packages", "total_weight_kg", 
            "avg_process_time_sec", "total_high_value_packages", "last_processed_at"
        ]]
        
        logger.info(f"Modelagem GOLD concluída. Total de grupos: {len(df_gold)}")
        
    except Exception as e:
        logger.error(f"Erro durante a transformação/modelagem Gold: {e}")
        raise

    # 3. CARGA GOLD
    try:
        load_to_postgres_gold(df_gold, engine)
        logger.info(f"Carga da Tabela Gold concluída com sucesso. O Pipeline Medallhão está 100% FUNCIONAL!")

    except Exception as e:
        logger.error(f"Erro durante a carga Gold no PostgreSQL: {e}")
        raise
    
if __name__ == "__main__":
    run_etl_gold()