# 🚀 Logistic Data Lake — Airflow + MinIO + PostgreSQL + Soda Core

**Data Lake profissional modular**, desenvolvido para demonstrar uma arquitetura de ingestão, qualidade e processamento de dados seguindo o conceito **Medallion Architecture (RAW → BRONZE → SILVER → GOLD)**.  
Totalmente executável em ambiente local com **Airflow + Docker Compose + MinIO + PostgreSQL**, e pronto para escalar em ambientes corporativos.

> ⚠️ Este repositório é **privado** e destinado apenas para **demonstração interna**. Não deve ser compartilhado ou clonado publicamente.

---

## 🧠 Objetivo

Este projeto foi construído como um **demo corporativo de Data Lakehouse**, combinando:
- ingestão incremental,
- processamento particionado em Parquet,
- governança e qualidade de dados com **Soda Core**,
- orquestração automatizada com **Apache Airflow**.

> Ideal para demonstrações técnicas, POCs internas e ensino avançado de Engenharia de Dados aplicada à Logística 5.0.

## 🏗️ Arquitetura
      ┌────────────┐
      │   RAW      │  ← Dados brutos (CSV, JSON, APIs, etc.)
      └─────┬──────┘
            │
     Ingestão (Airflow + Python)
            │
      ┌─────▼──────┐
      │  BRONZE    │  ← Padronização, formatação, Parquet
      └─────┬──────┘
            │
     Limpeza / Validação (Soda Core)
            │
      ┌─────▼──────┐
      │  SILVER    │  ← Dados refinados, prontos para modelagem
      └─────┬──────┘
            │
     Agregações / SQL puro (PostgreSQL)
            │
      ┌─────▼──────┐
      │   GOLD     │  ← Data Warehouse analítico
      └────────────┘

---

## ⚙️ Stack Técnica

| Componente | Função | Observação |
|-------------|--------|-------------|
| **Apache Airflow 2.7+** | Orquestração | LocalExecutor com DAGs modulares |
| **PostgreSQL** | Metadados e camada GOLD | Consultas SQL otimizadas |
| **MinIO (S3 local)** | Armazenamento RAW/BRONZE/SILVER | Via `s3fs` e `boto3` |
| **Parquet + PyArrow** | Formato de dados | Alta performance e compressão |
| **Soda Core** | Data Quality | Regras e monitoramento de qualidade |
| **Python** | ETL e lógica de negócio | Pandas, PyArrow, Faker, Boto3 |

---

## 🧩 Estrutura de Pastas
📦 Logistic_Datalake
┣ 📂 dags/ → DAGs do Airflow (RAW, BRONZE, SILVER, GOLD, QA)
┣ 📂 scripts/ → Funções auxiliares e ETLs
┣ 📂 data/ → Dados particionados por camada (Parquet)
┣ 📂 soda/ → Arquivos de configuração e scans do Soda Core
┣ 📂 logs/ → Logs do Airflow (ignorado no Git)
┣ 📜 docker-compose.yml → Infraestrutura local completa
┣ 📜 requirements.txt → Dependências Python
┣ 📜 .env → Variáveis de ambiente (credenciais, paths)
┗ 📜 README.md 


---

## 🧰 Quick Start (Local)

### 1️⃣ — Ativar ambiente local
```powershell
cd "C:\Users\Luis Camargo\Desktop\Logistic_Datalake"
.venv\Scripts\Activate.ps1

2️⃣ — Subir a infraestrutura
docker-compose up -d

3️⃣ — Acessar interfaces

| Serviço           | URL                                            | Login padrão                    |
| ----------------- | ---------------------------------------------- | ------------------------------- |
| **Airflow UI**    | [http://localhost:8080](http://localhost:8080) | `daxlog123` / `daxlog123`       |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `daxlog123` / `daxlog123`       |
| **PostgreSQL**    | localhost:5432                                 | DB: `gold_dw` / user: `airflow` |

🧮 Qualidade de Dados — Soda Core

Após a ingestão na camada BRONZE, executa-se validações automáticas:

Consistência de schema

Campos nulos ou duplicados

Regras de negócio customizadas

Exemplo de execução manual de scan:

soda scan -d postgres -c soda/config.yml soda/checks.yml

📈 Futuro e Extensões

Integração com dbt-core para modelagem SQL moderna

Deploy remoto em ambientes corporativos (Azure, AWS, GCP)

Adição de camadas Streaming (Kafka) e Monitoring (Grafana/Prometheus) 

🧾 Licença

MIT License — uso interno para demonstração e aprendizado.
