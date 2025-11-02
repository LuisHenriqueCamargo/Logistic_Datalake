FROM apache/airflow:2.7.3-python3.11

# 1️⃣ Modo root apenas para libs de sistema
USER root

# Instala bibliotecas de sistema necessárias e o Java (JDK)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    postgresql-client \
    curl \
    openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ESSENCIAL: Adiciona o wait-for-it.sh, necessário para o entrypoint do init_airflow
ADD https://github.com/vishnubob/wait-for-it/raw/master/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# 2️⃣ Volta para usuário airflow antes de instalar Python deps
USER airflow

COPY requirements.txt /requirements.txt

# Instala as dependências Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# 3️⃣ Instalação do dbt Core
# Adiciona o dbt Core e o adaptador Postgres ao ambiente Airflow
RUN pip install --no-cache-dir dbt-postgres
