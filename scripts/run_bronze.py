"""Executa a camada Bronze sem o Airflow (usado no CI e para depuração).

Lê as mesmas variáveis de conexão do Airflow (AIRFLOW_CONN_<CONN_ID>); sem
elas, conecta no Postgres local com a senha de PGPASSWORD.

    python scripts/run_bronze.py               # todas as origens
    python scripts/run_bronze.py --source gama # uma origem
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from medallion.bronze import ingest_entity  # noqa: E402
from medallion.config import load_sources  # noqa: E402

DEFAULT = "postgresql://postgres@localhost:5432/{db}"  # senha via PGPASSWORD


def dsn_for(conn_id: str, default_db: str) -> str:
    uri = os.getenv(f"AIRFLOW_CONN_{conn_id.upper()}", DEFAULT.format(db=default_db))
    return uri.replace("postgres://", "postgresql://", 1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="nome da origem em config/sources.yaml")
    args = parser.parse_args()

    sources = [s for s in load_sources() if not args.source or s.name == args.source]
    with psycopg2.connect(dsn_for("dw", "dw")) as dw_conn:
        for source in sources:
            with psycopg2.connect(dsn_for(source.conn_id, source.name)) as src_conn:
                for entity in source.entities:
                    ingest_entity(src_conn, dw_conn, source, entity)


if __name__ == "__main__":
    main()
