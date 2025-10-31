# Logistic Data Lake — Professional Demo (Airflow + MinIO + Databricks-ready)

## Objetivo
Projeto demonstrativo corporativo para pipeline Data Lake seguindo Logística 5.0. Arquitetura modular: RAW → BRONZE → SILVER → GOLD. Permite demonstração local com MinIO e execução em Azure Databricks para processamento em larga escala.

## Stack
- Apache Airflow 2.7 (LocalExecutor)
- MinIO (S3 local)
- PostgreSQL (Airflow metadata + Gold DW)
- MongoDB (raw optional)
- Azure Databricks (opcional) — integração via `DatabricksSubmitRunOperator`
- Parquet (pyarrow) + partitioning year/month/day
- Python: pandas, pyarrow, s3fs, boto3, faker

## Quick start (local)
1. Criar pastas:
   ```bash
   mkdir dags scripts data logs
