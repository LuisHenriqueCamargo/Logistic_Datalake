# 🚀 Data Lake Logístico — Demonstração Interna

Este projeto é uma **demonstração corporativa de Data Lake**, baseada na arquitetura **Medallion (RAW → BRONZE → SILVER → GOLD)**, totalmente apresentada em ambiente local com:

* **Apache Airflow** para orquestração modular;
* **PostgreSQL** (metadados e camada GOLD);
* **MinIO** (armazenamento local S3 para BRONZE e SILVER);
* **Parquet + PyArrow** para formatação de dados;
* **Soda Core** para Data Quality e desempenho máximo.

> ⚠️ **Aviso:** Este repositório é **privado** e destinado apenas para demonstração interna. Não deve ser compartilhado publicamente.

---

## 🎯 Objetivo

Montar como projetos, validar e executar pipelines de Data Lake corporativos, permitindo:

* Ingestão incremental de dados brutos;
* Processamento particionado em Parquet;
* Monitoramento de qualidade de dados com Soda Core;
* Gerenciamento de tabelas de metadados;
* Orquestração de fluxo de dados com Airflow.

---

## 🏗️ Arquitetura

┌──────────┐
│   RAW    │  ← Dados brutos (CSV, JSON, APIs, etc.)
└─────┬────┘
      │
Ingestão (Airflow + Python)
      │
┌─────▼────┐
│  BRONZE  │  ← Padronização, formatação, Parquet
└─────┬────┘
      │
Limpeza / Validação (Soda Core)
      │
┌─────▼────┐
│  SILVER  │  ← Dados refinados, prontos para modelagem
└─────┬────┘
      │
Agregações / SQL puro (PostgreSQL)
      │
┌─────▼────┐
│   GOLD   │  ← Data Warehouse analítico
└──────────┘

## ⚙️ Técnica de Pilha

| Componente | Função Principal | Detalhes Técnicos |
|:-------------|:-----------------|:-------------------|
| **Apache Airflow 2.7+** | Orquestração de Pipelines | LocalExecutor com DAGs modulares |
| **PostgreSQL** | Metadados e Camada GOLD | Consultas SQL otimizadas para DW |
| **MinIO (S3 local)** | Armazenamento do Data Lake | RAW, BRONZE, SILVER via `s3fs` e `boto3` |
| **Parquet + PyArrow** | Formato de Dados | Alta performance e compressão |
| **Soda Core** | Data Quality | Definição de regras e monitoramento |
| **Python** | ETL e Lógica de Negócio | Pandas, PyArrow, Faker, Boto3 |

---

## 🗂️ Estrutura de Pastas

```
📦 Logistic_Datalake
┣ 📂 dags/ → DAGs do Airflow (RAW, BRONZE, SILVER, GOLD, QA)
┣ 📂 scripts/ → Funções auxiliares e scripts de ETL
┣ 📂 data/ → Dados particionados por camada (Parquet)
┣ 📂 soda/ → Arquivos de configuração e scans do Soda Core
┣ 📂 logs/ → Logs do Airflow (Ignorado no Git)
┣ 📜 docker-compose.yml → Infraestrutura local completa
┣ 📜 requirements.txt → Dependências Python
┣ 📜 .env → Variáveis de ambiente (credenciais, paths)
┗ 📜 README.md
```

---

## 🚀 Início Rápido (Local)

### 1️⃣ — Ativar ambiente Python

Abra o terminal na pasta raiz do projeto e execute:

```powershell
# Exemplo de ativação de ambiente virtual no PowerShell
cd "C:\Users\Luis Camargo\Desktop\Logistic_Datalake"
.venv\Scripts\Activate.ps1
```

### 2️⃣ — Subir a infraestrutura completa

Utilize o Docker Compose para iniciar todos os serviços (Airflow, MinIO, PostgreSQL):

```bash
docker-compose up -d
```

### 3️⃣ — Acessar as interfaces

| Serviço | URL | Login Padrão |
|:---|:---|:---|
| **Airflow UI** | [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080) | `daxlog123` / `daxlog123` |
| **MinIO Console** | [http://localhost:9001](https://www.google.com/search?q=http://localhost:9001) | `daxlog123` / `daxlog123` |
| **PostgreSQL** | `localhost:5432` | DB: `gold_dw` / User: `airflow` |

---

## 📊 Qualidade de Dados — Soda Core

Após a ingestão na camada BRONZE, o Soda Core executa validações automáticas:

* Consistência de *schema*
* Verificação de campos nulos ou duplicados
* Aplicação de regras de negócio customizadas

**Exemplo de execução manual de scan:**

```bash
soda scan -d postgres -c soda/config.yml soda/checks.yml
```

---

## 📈 Futuro e Extensões

Este projeto é modular e possui potencial para as seguintes evoluções:

* Integração com **dbt-core** para modelagem SQL moderna na camada SILVER/GOLD.
* *Deploy* remoto em ambientes corporativos (*cloud* como Azure, AWS, GCP).
* Adição de camadas Streaming (**Kafka**) e Monitoring (**Grafana/Prometheus**).

---

## 📜 Licença

MIT License — uso interno para demonstração e aprendizado.
