from pathlib import Path

import pytest

from medallion.config import ConfigError, dataset_uri, load_sources


def test_config_do_repositorio_e_valida():
    sources = {s.name: s for s in load_sources()}
    assert set(sources) == {"alfa", "beta", "gama"}
    for source in sources.values():
        assert {e.name for e in source.entities} == {"clientes", "contratos", "ordens_servico"}


def test_defaults_e_override_de_schedule():
    sources = {s.name: s for s in load_sources()}
    assert sources["alfa"].schedule == "15 * * * *"
    assert sources["gama"].schedule == "45 */2 * * *"
    assert sources["alfa"].retries == 2


def test_dataset_uri():
    assert dataset_uri("alfa", "clientes") == "medallion://bronze/alfa/clientes"


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "sources.yaml"
    path.write_text(body, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("sources: {}", "nenhuma origem"),
        ("sources:\n  x:\n    conn_id: c\n    schedule: '@daily'\n    entities: []", "ao menos uma"),
        (
            "sources:\n  x:\n    conn_id: c\n    schedule: '@daily'\n    entities:\n      - {name: a, table: t}",
            "primary_key",
        ),
        (
            "sources:\n  x:\n    conn_id: c\n    schedule: '@daily'\n    entities:\n"
            "      - {name: a, table: 't; drop table x', primary_key: [id]}",
            "identificador",
        ),
        (
            "sources:\n  x:\n    conn_id: c\n    entities:\n      - {name: a, table: t, primary_key: [id]}",
            "schedule",
        ),
    ],
)
def test_config_invalida_e_rejeitada(tmp_path, body, message):
    with pytest.raises(ConfigError, match=message):
        load_sources(_write(tmp_path, body))
