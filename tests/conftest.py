import os
import uuid

import psycopg2
import pytest

PG_URL = os.getenv("TEST_PG_URL", "postgresql://postgres@localhost:5432/postgres")  # senha via PGPASSWORD


@pytest.fixture
def pg():
    """Banco descartável por teste: origem e DW no mesmo banco, schemas separados."""
    try:
        admin = psycopg2.connect(PG_URL, connect_timeout=3)
    except psycopg2.OperationalError:
        pytest.skip("Postgres indisponível (defina TEST_PG_URL)")
    admin.autocommit = True
    name = f"test_{uuid.uuid4().hex[:8]}"
    with admin.cursor() as cur:
        cur.execute(f"CREATE DATABASE {name}")
    url = PG_URL.rsplit("/", 1)[0] + f"/{name}"
    src, dw = psycopg2.connect(url), psycopg2.connect(url)
    try:
        yield src, dw
    finally:
        src.close()
        dw.close()
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE {name} WITH (FORCE)")
        admin.close()
