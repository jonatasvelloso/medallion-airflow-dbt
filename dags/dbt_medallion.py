"""Silver e Gold em dbt, orquestradas pelo Astronomer Cosmos.

O Cosmos transforma cada model e cada teste do projeto dbt em uma task do
Airflow: dá para ver, reexecutar e depurar um model isolado na UI.

A DAG não tem horário fixo. Ela é disparada pelos Datasets publicados pela
Bronze (orquestração orientada a dados): roda assim que todas as
entidades consumidas pelo dbt tiverem sido atualizadas desde a última
execução.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import Dataset
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping

from medallion.config import dataset_uri, load_sources

DBT_PROJECT = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/home/airflow/dbt_venv/bin/dbt")

bronze_datasets = [
    Dataset(dataset_uri(source.name, entity.name)) for source in load_sources() for entity in source.entities
]

dbt_medallion = DbtDag(
    dag_id="dbt_medallion",
    project_config=ProjectConfig(DBT_PROJECT, install_dbt_deps=False),
    profile_config=ProfileConfig(
        profile_name="medallion",
        target_name="airflow",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="dw",
            profile_args={"schema": "public", "threads": 4},
        ),
    ),
    # dbt roda em um virtualenv próprio dentro da imagem: as dependências do
    # dbt não disputam versões com as do Airflow.
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
    # Testes rodam logo após cada model: um model quebrado bloqueia os
    # dependentes antes de propagar erro para a Gold.
    render_config=RenderConfig(test_behavior=TestBehavior.AFTER_EACH),
    # Os Datasets desta DAG são os de entrada (Bronze); as tabelas geradas
    # pelo dbt não precisam publicar Datasets próprios.
    operator_args={"emit_datasets": False},
    schedule=bronze_datasets,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "analytics-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["silver", "gold", "dbt"],
)
