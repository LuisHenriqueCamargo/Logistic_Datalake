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



 ![1732023052836](https://github.com/user-attachments/assets/7e301022-c502-4104-948f-c9c55eb3d189)



## ⚙️ Provas Visuais da Execução
Orquestração e Pipeline (Airflow)

 O Airflow gerencia e automatiza a execução de cada estágio (RAW, BRONZE, SILVER, GOLD), garantindo a confiabilidade do pipeline.
<img width="1888" height="971" alt="image" src="https://github.com/user-attachments/assets/90967efb-d96a-4fa7-87ee-099e12750baf" /> 

Data Lake Storage (MinIO)

Utilização do MinIO para simular um S3, garantindo o armazenamento imutável e particionado das camadas BRONZE e SILVER.
<img width="1913" height="985" alt="image" src="https://github.com/user-attachments/assets/dd1ad11c-8885-463d-bd92-727170b43511" /> 

Resultado Final (Camada GOLD)

A camada GOLD contém os dados modelados e agregados, prontos para consumo por ferramentas de BI, comprovando a entrega final do projeto.
<img width="1918" height="1027" alt="image" src="https://github.com/user-attachments/assets/59df0de0-eabf-44d3-9477-edbd78e6104a" />




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
<img width="1458" height="350" alt="image" src="https://github.com/user-attachments/assets/2d246e63-6261-4110-84b8-2ab695c6de51" /> 


### 3️⃣ — Acessar as interfaces

| Serviço | URL | Login Padrão |
|:---|:---|:---|
| **Airflow UI** | [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080) | `daxlog123` / `daxlog123` |
| **MinIO Console** | [http://localhost:9001](https://www.google.com/search?q=http://localhost:9001) | `daxlog123` / `daxlog123` |
| **PostgreSQL** | `localhost:5432` | DB: `gold_dw` / User: `airflow` |

--- 



<img width="1908" height="808" alt="image" src="https://github.com/user-attachments/assets/b11731f6-48f3-479d-99a7-b82c87f29f3c" />


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
