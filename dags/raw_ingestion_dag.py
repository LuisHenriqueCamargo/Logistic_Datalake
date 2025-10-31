import logging
import random
import json
from datetime import datetime, timedelta
from uuid import uuid4
from decimal import Decimal # Necessário para o JSON Encoder

# Importações do Airflow
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# Importações de utilitários
from s3_utils import ensure_bucket, upload_bytes, BUCKET
from faker import Faker 

# Configurações
ROWS_TO_GENERATE = 200 # 200 linhas de dados logísticos robustos
logger = logging.getLogger(__name__)
fake = Faker('pt_BR')

# =========================================================================
# SOLUÇÃO DEFINITIVA: JSON ENCODER CUSTOMIZADO (MANTIDO INTACTO!)
# =========================================================================
class DecimalEncoder(json.JSONEncoder):
    """
    Trata tipos não serializáveis pelo JSON padrão, convertendo-os para float (Decimal) 
    ou string (datetime/timedelta).
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Mantido, embora não usemos mais Lat/Lon, garante a robustez
            return float(obj)
        if isinstance(obj, (datetime, timedelta)):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)



# DADOS DE APOIO AJUSTADOS PARA CROSS-DOCKING / SORTER

SORTATION_HUBS = [
    {"name": "HUB_SP_CENTRAL", "city": "São Paulo"},
    {"name": "HUB_RJ_GATE", "city": "Rio de Janeiro"},
    {"name": "HUB_MG_DIST", "city": "Belo Horizonte"},
    {"name": "HUB_PR_SUL", "city": "Curitiba"},
]
CARRIERS = ["Transportadora Alfa", "Transportadora Beta", "Frota Própria"]
SCAN_TYPES = ["Inbound", "Outbound", "Re-scan", "Exception Scan"]
SORTATION_STATUSES = ["OK", "Diverted", "Mis-sort", "Damaged", "Missing Label"]
PRODUCT_CATEGORIES = ['Eletrônicos', 'Alimentos Secos', 'Vestuário', 'Industrial', 'Documentos']


def generate_and_upload_raw_data():
    """
    Função de ETL que gera dados de Bip de Esteira Sorter/Cross-docking.
    """
    logger.info("--- Iniciando a Tarefa de Ingestão Bruta (200 linhas de Sorter/AWB) ---")
    
    # 1. Garante a criação do bucket
    ensure_bucket(BUCKET)

    # 2. Geração de Dados Falsos Robustos (Cross-docking/Sorter)
    records = []
    
    for i in range(ROWS_TO_GENERATE):
        scan_status = random.choice(SORTATION_STATUSES)
        
        origin_hub = random.choice(SORTATION_HUBS)
        # Garante que o destino seja diferente da origem
        destination_hub = random.choice([h for h in SORTATION_HUBS if h != origin_hub]) 

        is_exception = (scan_status in ["Diverted", "Mis-sort", "Damaged", "Missing Label"])
        scan_type = "Exception Scan" if is_exception else random.choice(["Inbound", "Outbound", "Re-scan"])


        record = {
            "event_id": str(uuid4()), 
            "scan_timestamp": (datetime.now() - timedelta(seconds=random.randint(10, 300))), # Evento de bip
            
            # Dados de Rastreamento (AWB)
            "awb_number": fake.bothify(text='AWB-##########'), # Air Waybill Number (chave principal)
            "carrier_name": random.choice(CARRIERS),
            "sortation_line_id": fake.bothify(text='LINE-##'),
            "scanner_id": fake.bothify(text='SCN-###'),

            # Dados do Pacote (Mais foco em peso/dimensão)
            "product_category": random.choice(PRODUCT_CATEGORIES),
            "package_weight_kg": round(random.uniform(0.5, 30.0), 2),
            "package_dimension_cm": f"{fake.random_int(10, 60)}x{fake.random_int(10, 60)}x{fake.random_int(10, 60)}",
            "declared_value": round(random.uniform(50.0, 5000.0), 2),
            
            # KPIs de Cross-docking/Sorter
            "scan_type": scan_type,
            "scan_status": scan_status,
            "origin_hub": origin_hub["name"],
            "destination_hub": destination_hub["name"],
            "process_time_sec": round(random.uniform(0.1, 5.0), 2), # Tempo de processamento na esteira
            "is_exception": is_exception, 
        }
        records.append(record)
        
    logger.info(f"Gerados {ROWS_TO_GENERATE} registros de Sorter/AWB robustos.")

    # 3. Processamento e Upload (A Lógica que JÁ FUNCIONA)
    try:
        # Serializa para JSON Lines usando o encoder customizado!
        json_lines = "\n".join([json.dumps(r, cls=DecimalEncoder) for r in records]) 
        data_bytes = json_lines.encode('utf-8')
        
        # Cria a chave S3
        today = datetime.now()
        # O nome do arquivo reflete o novo tema
        key_name = f"raw/awb_sortation_{today.strftime('%Y%m%d%H%M%S')}_{str(uuid4())}.json"
        
        # Upload para o MinIO
        upload_bytes(BUCKET, key_name, data_bytes, content_type="application/json")
        
        logger.info(f"Upload bem-sucedido: s3://{BUCKET}/{key_name}")
    except Exception as e:
        logger.error(f"Falha CRÍTICA durante o upload para MinIO: {e}")
        raise e

# --- Definição da DAG ---
with DAG(
    dag_id="ingestao_bruta",
    start_date=datetime(2025, 10, 28),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "daxlog",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["raw", "ingestion", "corporativo", "awb"],
) as dag:
    
    generate_raw = PythonOperator(
        task_id="gerar_bruto",
        python_callable=generate_and_upload_raw_data,
    )