<h1 align="center">👋 Olá, eu sou <strong>Luis Camargo</strong></h1>
<h3 align="center">Especialista em Logística e Engenharia de Dados</h3>

<p align="center">
  <a href="https://www.linkedin.com/in/luisespecialista/" target="_blank">
    <img src="https://img.shields.io/badge/LinkedIn-blue?logo=linkedin&logoColor=white" alt="LinkedIn"/>
  </a>
  <a href="mailto:especialista.luiscamargo@gmail.com">
    <img src="https://img.shields.io/badge/Email-especialista.luiscamargo%40gmail.com-red?logo=gmail&logoColor=white" alt="Email"/>
  </a>
  <a href="https://wa.me/5511940880735">
    <img src="https://img.shields.io/badge/WhatsApp-Contato-brightgreen?logo=whatsapp&logoColor=white" alt="WhatsApp"/>
  </a>
</p>

---

## 🚀 Logistic Data Lake — Demonstração Interna

Este projeto é uma **demonstração corporativa de Data Lake**, baseado na arquitetura **Medallion (RAW → BRONZE → SILVER → GOLD)**, totalmente executável em ambiente local com:

- **Airflow** (orquestração e automação de pipelines)
- **PostgreSQL** (metadados do Airflow + camada GOLD)
- **MinIO** (armazenamento RAW, BRONZE e SILVER em Parquet)
- **Soda Core** (monitoramento e validação de qualidade de dados)
- **SQL puro e Python** para máxima performance

> ⚠️ Este repositório é **privado** e destinado apenas a demonstração interna. Não deve ser compartilhado publicamente.

---

## 🧩 Objetivo

Mostrar **como projetar, validar e executar pipelines de Data Lake** corporativos, permitindo:

- Ingestão incremental de dados brutos
- Processamento e padronização em Parquet
- Monitoramento de qualidade de dados com Soda Core
- Transformações e agregações em SQL puro
- Orquestração de fluxo de dados com Airflow

---

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
     Validação (Soda Core)
            │
      ┌─────▼──────┐
      │  SILVER    │  ← Dados refinados e prontos para modelagem
      └─────┬──────┘
            │
     Agregações / SQL puro (PostgreSQL)
            │
      ┌─────▼──────┐
      │   GOLD     │  ← Data Warehouse analítico
      └────────────┘

      
---

## ⚙️ Stack Técnica
Componente,Função Principal,Detalhes Técnicos
Apache Airflow 2.7+,Orquestração de Pipelines,LocalExecutor com DAGs modulares
PostgreSQL,Metadados e Camada GOLD,Consultas SQL otimizadas para DW
MinIO (S3 local),Armazenamento do Data Lake,"RAW, BRONZE, SILVER via s3fs e boto3"
Parquet + PyArrow,Formato de Dados,Alta performance e compressão
Soda Core,Data Quality,Definição de regras e monitoramento
Python,ETL e Lógica de Negócio,"Pandas, PyArrow, Faker, Boto3"

## 📂 Estrutura de Pastas

📦 Logistic_Datalake
┣ 📂 dags/ → DAGs Airflow (RAW → GOLD + QA)
┣ 📂 scripts/ → Funções auxiliares e ETLs
┣ 📂 data/ → Dados particionados por camada (Parquet)
┣ 📂 soda/ → Configuração e scans do Soda Core
┣ 📂 logs/ → Logs do Airflow (não versionados)
┣ 📜 docker-compose.yml → Infraestrutura local
┣ 📜 requirements.txt → Dependências Python
┣ 📜 .env → Variáveis de ambiente (credenciais)
┗ 📜 README.md


---

## 🧰 Quick Start (Local)

### 1️⃣ — Ativar ambiente Python
```powershell
cd "C:\Users\Luis Camargo\Desktop\Logistic_Datalake"
.venv\Scripts\Activate.ps1

2️⃣ — Subir infraestrutura completa
docker-compose up -d

3️⃣ — Acessar interfaces
| Serviço           | URL                                            | Login padrão                    |
| ----------------- | ---------------------------------------------- | ------------------------------- |
|       Airflow UI  | [http://localhost:8080](http://localhost:8080) | `daxlog123` / `daxlog123`       |
|     MinIO Console | [http://localhost:9001](http://localhost:9001) | `daxlog123` / `daxlog123`       |
|     PostgreSQL    | localhost:5432                                 | DB: `gold_dw` / user: `airflow` |

🧮 Qualidade de Dados — Soda Core

Após a camada BRONZE, os dados passam por validações automáticas de:
Consistência de schema
Campos nulos ou duplicados
Regras de negócio definidas
Executar manualmente scan local: 
soda scan -d postgres -c soda/config.yml soda/checks.yml

📈 Futuro & Extensões

Integração com dbt-core para transformação SQL modular
Deploy remoto em Azure, AWS ou GCP
Streaming de dados (Kafka) e monitoramento (Grafana/Prometheus) 


💼 Autor
<h4>Luis Henrique Camargo — Especialista em Logística e Engenharia de Dados</h4> <p align="center"> <a href="https://www.linkedin.com/in/luisespecialista/" target="_blank"> <img src="https://img.shields.io/badge/LinkedIn-blue?logo=linkedin&logoColor=white" alt="LinkedIn"/> </a> <a href="mailto:especialista.luiscamargo@gmail.com"> <img src="https://img.shields.io/badge/Email-especialista.luiscamargo%40gmail.com-red?logo=gmail&logoColor=white" alt="Email"/> </a> <a href="https://wa.me/5511940880735"> <img src="https://img.shields.io/badge/WhatsApp-Contato-brightgreen?logo=whatsapp&logoColor=white" alt="WhatsApp"/> </a> </p>

💡 “Transformar dados em inteligência e operações em vantagem competitiva.”
