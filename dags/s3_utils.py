import os
import boto3
from botocore.exceptions import ClientError
import logging

# Importação adicional necessária para o Config do boto3
import boto3.session 

# Configuração do Logger
logger = logging.getLogger(__name__)

# Configurações do ambiente, obtidas das variáveis do Docker Compose
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "daxlog123")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "daxlog123")
BUCKET = os.getenv("S3_BUCKET", "datalake")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1") # Definido para uso explícito

def get_minio_client(): # Renomeado para uso consistente nas importações
    """
    Configura o cliente boto3 para o MinIO.
    Ajuste: Força a versão de assinatura 's3v4' explicitamente para garantir
    o sucesso do put_object no MinIO, mesmo com 'path' style.
    """
    logger.info(f"Conectando ao MinIO em: {S3_ENDPOINT} com BUCKET: {BUCKET}")
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        # Mantendo a região, pois ela é necessária para a assinatura S3v4
        region_name=AWS_REGION,
        config=boto3.session.Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
    )

def ensure_bucket(bucket=BUCKET):
    """ Cria o bucket se ele não existir, usando o boto3. """
    s3 = get_minio_client() # Chamando a função renomeada
    try:
        s3.head_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' já existe.")
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code in ('404', 'NoSuchBucket'):
            try:
                s3.create_bucket(Bucket=bucket)
                logger.info(f"Bucket '{bucket}' criado com sucesso.")
                return True
            except ClientError as e_create:
                logger.error(f"Falha ao criar bucket '{bucket}': {e_create}")
                raise e_create
        else:
            logger.error(f"Erro ao verificar bucket '{bucket}': {e}")
            raise

def upload_bytes(bucket, key, data_bytes, content_type="application/octet-stream"):
    """ Função de upload que garante o bucket e sobe o objeto. """
    # Não precisa do ensure_bucket aqui, pois ele é chamado na DAG antes.
    # Mas mantemos, pois é inofensivo e garante resiliência.
    ensure_bucket(bucket)
    s3 = get_minio_client() # Chamando a função renomeada
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data_bytes,
            ContentType=content_type
        )
        logger.info(f"Upload bem-sucedido: s3://{bucket}/{key}")
    except ClientError as e:
        logger.error(f"Falha CRÍTICA no upload para s3://{bucket}/{key}: {e}")
        # Lança o erro para que a tarefa seja marcada como fracassada no Airflow
        raise e

# =========================================================================
# FUNÇÕES ADICIONADAS PARA O ETL DE BRONZE (LEITURA)
# =========================================================================

def list_objects_in_raw(s3_client, bucket, prefix="raw/"):
    """ Encontra a chave do último arquivo (o mais recente) dentro da pasta RAW. """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        # Filtra e encontra a chave do arquivo mais recente baseado na LastModified
        if response.get('Contents'):
            all_objects = response['Contents']
            
            # Ponto Sênior: Retorna o objeto com a data de modificação mais recente
            latest_object = max(all_objects, key=lambda obj: obj['LastModified'])
            return latest_object['Key']
        
        return None
    except ClientError as e:
        logger.error(f"Erro ao listar objetos S3: {e}")
        # É seguro retornar None ou lançar a exceção dependendo da DAG.
        # Para Airflow, é melhor lançar para falhar a tarefa.
        raise

def get_latest_object_content(s3_client, bucket, key):
    """ Baixa o conteúdo do arquivo mais recente. """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except ClientError as e:
        logger.error(f"Erro ao baixar o objeto S3 '{key}': {e}")
        raise