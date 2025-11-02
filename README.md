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

- **📊 Dados & BI:** Parquet, PyArrow, Python (Pandas, NumPy), SQL  
- **⚙️ Engenharia de Dados:** Airflow, PostgreSQL, MinIO, Docker, ETL  
- **🧠 Data Quality:** Soda Core, validações automáticas de regras de negócio  
- **🌐 Automação & Workflow:** Python, DAGs Airflow, integração local com MinIO  
- **📈 Performance Logística:** Monitoramento de KPIs de SLA, custo e operação  

---

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

