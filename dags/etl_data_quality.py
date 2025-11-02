# Arquivo: dags/etl_data_quality.py (Versão COMPLETA e CORRIGIDA)

import logging
import os
from soda.scan import Scan 

logger = logging.getLogger(__name__)

# O caminho do diretório 'dags/'
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 

def run_soda_scan(table_name: str, checks_file: str):
    """
    Executa o Soda Scan na tabela especificada e falha se houver falhas críticas.
    """
    logger.info(f"Iniciando Soda Data Quality Scan na tabela: {table_name}")

    scan = Scan()
    
    # 1. Configura a conexão e os arquivos
    scan.set_data_source_name("postgres_daxlog")
    scan.add_configuration_yaml_file(os.path.join(BASE_DIR, "soda", "soda_config.yml"))
    
    # 2. Define o local dos checks e a tabela a ser escaneada
    # CORREÇÃO APLICADA: Substituído add_check_yaml_file por add_sodacl_yaml_file
    scan.add_sodacl_yaml_file(os.path.join(BASE_DIR, "soda", checks_file))
    scan.set_scan_definition_name(f"dq_scan_for_{table_name}")
    
    # 3. Executa o Scan (o resultado é um código de saída)
    result = scan.execute()
    
    logger.info("Soda Scan concluído. Analisando resultados...")

    # CORREÇÃO APLICADA: Verifica o código de saída (0=Sucesso, 2=Falha, 3=Erro)
    if result != 0:
        logger.error(f"❌ FALHA DE DATA QUALITY ou ERRO DE SCAN encontrado. Código de saída: {result}")
        for check in scan.get_checks():
            # A função get_checks funciona mesmo que has_check_failures não exista.
            if check.outcome and check.outcome.name == 'FAIL':
                logger.error(f"  - Check Falhou: {check.name} | Resultado: {check.outcome.name}")
        
        # Levanta exceção para FAZER a tarefa do Airflow falhar
        raise Exception(f"Falhas de Data Quality encontradas. Código de saída: {result}")
    
    else:
        logger.info(f"✅ Sucesso! Nenhum erro de Data Quality crítico encontrado para {table_name}.")
        
    return "Data Quality Aprovada"

if __name__ == "__main__":
    try:
        run_soda_scan("silver_enriched_awb_data", "silver_checks.yml")
    except Exception as e:
        print(f"Execução falhou: {e}")