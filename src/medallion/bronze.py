"""Ingestão de uma entidade da origem para a Bronze.

Fluxo de cada execução (tudo em uma transação no DW):

1. Garante schema/tabela na Bronze e adiciona colunas novas da origem
   (evolução de schema sem intervenção manual).
2. Lê a marca d'água da própria Bronze (maior valor da coluna de
   alteração) — sem estado escondido em Variables do Airflow.
3. Extrai com COPY (SELECT ...) TO STDOUT para um arquivo temporário.
4. Carrega o arquivo em uma tabela temporária com COPY FROM STDIN.
5. Roda as checagens de qualidade; se falharem, nada é gravado.
6. Faz upsert idempotente na Bronze, atualizando só linhas que mudaram.
7. Registra o lote em meta.ingestion_log.

A Bronze guarda todas as colunas como texto: é uma cópia fiel da origem.
Tipagem e regras de negócio acontecem no dbt (staging/silver/gold).
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from psycopg2 import sql

from medallion.config import Entity, Source
from medallion.quality import BatchProfile, check_profile, check_row_count, profile_query

log = logging.getLogger(__name__)

META_DDL = """
CREATE SCHEMA IF NOT EXISTS meta;
CREATE TABLE IF NOT EXISTS meta.ingestion_log (
    batch_id        text PRIMARY KEY,
    source          text NOT NULL,
    entity          text NOT NULL,
    mode            text NOT NULL,
    watermark       text,
    rows_extracted  integer NOT NULL,
    rows_upserted   integer NOT NULL,
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz NOT NULL
);
"""


@dataclass(frozen=True)
class IngestionResult:
    batch_id: str
    mode: str
    watermark: str | None
    rows_extracted: int
    rows_upserted: int


def _source_columns(cur, table: str) -> list[str]:
    cur.execute(sql.SQL("SELECT * FROM {} LIMIT 0").format(sql.Identifier(table)))
    return [d.name for d in cur.description]


def _ensure_bronze_table(cur, schema: str, entity: Entity, columns: list[str]) -> None:
    target = sql.Identifier(schema, entity.name)
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    cols = sql.SQL(", ").join(sql.SQL("{} text").format(sql.Identifier(c)) for c in columns)
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {target} (
                {cols},
                _batch_id    text NOT NULL,
                _ingested_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY ({pk})
            )
            """
        ).format(
            target=target,
            cols=cols,
            pk=sql.SQL(", ").join(map(sql.Identifier, entity.primary_key)),
        )
    )
    # Evolução de schema: coluna nova na origem vira coluna nova na Bronze.
    for column in columns:
        cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} text").format(target, sql.Identifier(column)))


def _watermark(cur, schema: str, entity: Entity) -> str | None:
    if not entity.is_incremental:
        return None
    cur.execute(
        sql.SQL("SELECT max({col}::timestamptz)::text FROM {target}").format(
            col=sql.Identifier(entity.incremental_by),
            target=sql.Identifier(schema, entity.name),
        )
    )
    return cur.fetchone()[0]


def _extract_query(src_cur, entity: Entity, watermark: str | None) -> str:
    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(entity.table))
    if watermark:
        # >= em vez de >: registros com o mesmo timestamp da última carga são
        # relidos e o upsert torna a repetição inofensiva.
        query = sql.SQL("{} WHERE {} >= {}").format(
            query, sql.Identifier(entity.incremental_by), sql.Literal(watermark)
        )
    return sql.SQL("COPY ({}) TO STDOUT WITH (FORMAT csv, HEADER true)").format(query).as_string(src_cur)


def _upsert(cur, schema: str, entity: Entity, columns: list[str], batch_id: str) -> int:
    target = sql.Identifier(schema, entity.name)
    col_ids = [sql.Identifier(c) for c in columns]
    non_keys = [c for c in columns if c not in entity.primary_key]
    updates = sql.SQL(", ").join(
        [sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in non_keys]
        + [sql.SQL("_batch_id = EXCLUDED._batch_id"), sql.SQL("_ingested_at = now()")]
    )
    changed = sql.SQL("({}) IS DISTINCT FROM ({})").format(
        sql.SQL(", ").join(sql.SQL("{}.{}").format(target, sql.Identifier(c)) for c in non_keys),
        sql.SQL(", ").join(sql.SQL("EXCLUDED.{}").format(sql.Identifier(c)) for c in non_keys),
    )
    cur.execute(
        sql.SQL(
            """
            INSERT INTO {target} ({cols}, _batch_id)
            SELECT {cols}, %s FROM _staging
            ON CONFLICT ({pk}) DO UPDATE SET {updates}
            WHERE {changed}
            """
        ).format(
            target=target,
            cols=sql.SQL(", ").join(col_ids),
            pk=sql.SQL(", ").join(map(sql.Identifier, entity.primary_key)),
            updates=updates,
            changed=changed,
        ),
        (batch_id,),
    )
    return cur.rowcount


def ingest_entity(source_conn, dw_conn, source: Source, entity: Entity) -> IngestionResult:
    """Ingere uma entidade. As conexões são psycopg2 (Airflow injeta via hook).

    Qualquer erro desfaz a transação no DW: a Bronze nunca fica pela metade.
    """
    try:
        return _ingest(source_conn, dw_conn, source, entity)
    except Exception:
        dw_conn.rollback()
        source_conn.rollback()
        raise


def _ingest(source_conn, dw_conn, source: Source, entity: Entity) -> IngestionResult:
    label = f"{source.name}.{entity.name}"
    batch_id = f"{label}:{uuid.uuid4().hex[:12]}"
    started_at = datetime.now(UTC)
    schema = source.bronze_schema

    with source_conn.cursor() as src, dw_conn.cursor() as dw:
        src.execute("SET TIME ZONE 'UTC'")
        dw.execute("SET TIME ZONE 'UTC'")
        dw.execute(META_DDL)

        columns = _source_columns(src, entity.table)
        missing = set(entity.primary_key) - set(columns)
        if missing:
            raise ValueError(f"{label}: chave {sorted(missing)} não existe em {entity.table}")
        _ensure_bronze_table(dw, schema, entity, columns)
        watermark = _watermark(dw, schema, entity)
        mode = "incremental" if watermark else "full"

        with tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024, mode="w+b") as buffer:
            src.copy_expert(_extract_query(src, entity, watermark), buffer)
            buffer.seek(0)
            dw.execute(
                sql.SQL("CREATE TEMP TABLE _staging ON COMMIT DROP AS SELECT {} FROM {} WITH NO DATA").format(
                    sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier(schema, entity.name)
                )
            )
            dw.copy_expert(
                sql.SQL("COPY _staging ({}) FROM STDIN WITH (FORMAT csv, HEADER true)")
                .format(sql.SQL(", ").join(map(sql.Identifier, columns)))
                .as_string(dw),
                buffer,
            )

        dw.execute(profile_query(sql.Identifier("_staging"), entity.primary_key))
        profile = BatchProfile(*dw.fetchone())
        check_row_count(profile.rows, entity.min_rows, mode == "incremental", label)
        check_profile(profile, label)

        upserted = _upsert(dw, schema, entity, columns, batch_id) if profile.rows else 0
        dw.execute(
            "INSERT INTO meta.ingestion_log VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())",
            (batch_id, source.name, entity.name, mode, watermark, profile.rows, upserted, started_at),
        )
    dw_conn.commit()
    source_conn.rollback()  # leitura apenas: encerra a transação na origem

    log.info("%s: modo=%s extraídas=%s gravadas=%s", label, mode, profile.rows, upserted)
    return IngestionResult(batch_id, mode, watermark, profile.rows, upserted)
