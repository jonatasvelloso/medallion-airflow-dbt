"""As DAGs carregam sem erro e respeitam a declaração do sources.yaml."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
pytest.importorskip("cosmos")

from airflow.models import DagBag  # noqa: E402

from medallion.config import dataset_uri, load_sources  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_sem_erros_de_importacao(dagbag):
    assert dagbag.import_errors == {}


def test_uma_dag_bronze_por_origem(dagbag):
    for source in load_sources():
        dag = dagbag.get_dag(f"bronze_{source.name}")
        assert dag is not None
        assert {t.task_id for t in dag.tasks} == {f"ingest_{e.name}" for e in source.entities}
        assert dag.catchup is False


def test_cada_task_bronze_publica_seu_dataset(dagbag):
    for source in load_sources():
        dag = dagbag.get_dag(f"bronze_{source.name}")
        for entity in source.entities:
            outlets = dag.get_task(f"ingest_{entity.name}").outlets
            assert [o.uri for o in outlets] == [dataset_uri(source.name, entity.name)]


def test_dbt_e_disparado_pelos_datasets_da_bronze(dagbag):
    dag = dagbag.get_dag("dbt_medallion")
    assert dag is not None
    esperados = {dataset_uri(s.name, e.name) for s in load_sources() for e in s.entities}
    assert {d.uri for d in dag.timetable.dataset_condition.objects} == esperados
    task_ids = {t.task_id for t in dag.tasks}
    assert any("mart_base_mensal" in t for t in task_ids)
