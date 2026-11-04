"""Testes de integração da ingestão Bronze contra um Postgres real."""

import pytest

from medallion.bronze import ingest_entity
from medallion.config import Entity, Source
from medallion.quality import DataQualityError

SOURCE = Source(name="teste", conn_id="src_teste", schedule="@hourly", retries=0, retry_delay_minutes=1)
FULL = Entity(name="clientes", table="cliente", primary_key=("id",), min_rows=2)
INCREMENTAL = Entity(name="contratos", table="contrato", primary_key=("id",), incremental_by="atualizado_em")


def _setup(src):
    with src.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE cliente (id int, nome text);
            INSERT INTO cliente VALUES (1, 'Ana'), (2, 'Bruno'), (3, 'Carla');
            CREATE TABLE contrato (id int PRIMARY KEY, status text, atualizado_em timestamp);
            INSERT INTO contrato VALUES (1, 'ativo', '2026-01-01 10:00'), (2, 'ativo', '2026-01-02 10:00');
            """
        )
    src.commit()


def _bronze(dw, table):
    with dw.cursor() as cur:
        cur.execute(f"SELECT * FROM bronze_teste.{table} ORDER BY id::int")
        return cur.fetchall()


def test_carga_completa_e_idempotente(pg):
    src, dw = pg
    _setup(src)
    first = ingest_entity(src, dw, SOURCE, FULL)
    assert (first.mode, first.rows_extracted, first.rows_upserted) == ("full", 3, 3)
    second = ingest_entity(src, dw, SOURCE, FULL)
    assert second.rows_upserted == 0, "reexecução não deve regravar linhas iguais"
    assert len(_bronze(dw, "clientes")) == 3


def test_carga_incremental_le_apenas_alteracoes(pg):
    src, dw = pg
    _setup(src)
    assert ingest_entity(src, dw, SOURCE, INCREMENTAL).mode == "full"
    with src.cursor() as cur:
        cur.execute("UPDATE contrato SET status='cancelado', atualizado_em='2026-01-05 09:00' WHERE id=1")
    src.commit()
    result = ingest_entity(src, dw, SOURCE, INCREMENTAL)
    assert result.mode == "incremental"
    assert result.watermark.startswith("2026-01-02 10:00")
    assert result.rows_upserted == 1
    assert _bronze(dw, "contratos")[0][1] == "cancelado"


def test_coluna_nova_na_origem_entra_na_bronze(pg):
    src, dw = pg
    _setup(src)
    ingest_entity(src, dw, SOURCE, FULL)
    with src.cursor() as cur:
        cur.execute("ALTER TABLE cliente ADD COLUMN email text; UPDATE cliente SET email='ana@x.com' WHERE id=1")
    src.commit()
    ingest_entity(src, dw, SOURCE, FULL)
    with dw.cursor() as cur:
        cur.execute("SELECT email FROM bronze_teste.clientes WHERE id='1'")
        assert cur.fetchone()[0] == "ana@x.com"


def test_lote_invalido_nao_altera_a_bronze(pg):
    src, dw = pg
    _setup(src)
    ingest_entity(src, dw, SOURCE, FULL)
    with src.cursor() as cur:
        cur.execute("UPDATE cliente SET nome='Alterado'; INSERT INTO cliente VALUES (1, 'Duplicado')")
    src.commit()
    with pytest.raises(DataQualityError, match="duplicadas"):
        ingest_entity(src, dw, SOURCE, FULL)
    assert {row[1] for row in _bronze(dw, "clientes")} == {"Ana", "Bruno", "Carla"}


def test_volume_minimo_bloqueia_carga_completa(pg):
    src, dw = pg
    _setup(src)
    with src.cursor() as cur:
        cur.execute("DELETE FROM cliente WHERE id > 1")
    src.commit()
    with pytest.raises(DataQualityError, match="mínimo"):
        ingest_entity(src, dw, SOURCE, FULL)
