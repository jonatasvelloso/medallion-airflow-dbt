"""DAG factory da camada Bronze.

Gera uma DAG por origem declarada em config/sources.yaml e uma task por
entidade. Cada task publica um Airflow Dataset ao terminar, e são esses
Datasets que disparam o dbt (dags/dbt_medallion.py): a Silver/Gold roda
quando os dados chegam, não em um horário fixo.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from medallion.config import Entity, Source, dataset_uri, load_sources

DW_CONN_ID = "dw"


def _ingest_task(source: Source, entity: Entity):
    @task(
        task_id=f"ingest_{entity.name}",
        outlets=[Dataset(dataset_uri(source.name, entity.name))],
        doc_md=f"Ingestão `{source.name}.{entity.table}` → `{source.bronze_schema}.{entity.name}` "
        f"({'incremental por ' + entity.incremental_by if entity.is_incremental else 'carga completa'}).",
    )
    def ingest() -> dict:
        from medallion.bronze import ingest_entity

        src = PostgresHook(postgres_conn_id=source.conn_id).get_conn()
        dw = PostgresHook(postgres_conn_id=DW_CONN_ID).get_conn()
        try:
            result = ingest_entity(src, dw, source, entity)
        finally:
            src.close()
            dw.close()
        return result.__dict__

    return ingest()


def build_bronze_dag(source: Source) -> DAG:
    with DAG(
        dag_id=f"bronze_{source.name}",
        description=source.description,
        schedule=source.schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "data-engineering",
            "retries": source.retries,
            "retry_delay": timedelta(minutes=source.retry_delay_minutes),
        },
        tags=["bronze", source.name],
    ) as dag:
        for entity in source.entities:
            _ingest_task(source, entity)
    return dag


for _source in load_sources():
    globals()[f"bronze_{_source.name}"] = build_bronze_dag(_source)
