import pytest

from medallion.quality import BatchProfile, DataQualityError, check_profile, check_row_count


def test_carga_completa_abaixo_do_minimo_falha():
    with pytest.raises(DataQualityError, match="mínimo esperado 100"):
        check_row_count(10, 100, incremental=False, label="alfa.clientes")


def test_carga_incremental_vazia_e_valida():
    check_row_count(0, 100, incremental=True, label="alfa.contratos")


def test_lote_limpo_passa():
    check_profile(BatchProfile(rows=10, null_keys=0, duplicated_keys=0), "x")


def test_lote_com_chave_nula_e_duplicada_falha():
    with pytest.raises(DataQualityError) as exc:
        check_profile(BatchProfile(rows=10, null_keys=1, duplicated_keys=2), "x")
    assert "chave primária nula" in str(exc.value)
    assert "duplicadas" in str(exc.value)
