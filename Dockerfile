FROM apache/airflow:2.10.4-python3.12

# Dependências do Airflow (Cosmos + gerador de dados), respeitando os
# constraints oficiais da versão para não quebrar o scheduler.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"

# dbt em virtualenv isolado: o Cosmos chama este executável.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
