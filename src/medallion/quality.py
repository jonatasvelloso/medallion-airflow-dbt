"""Checagens de qualidade executadas antes de qualquer dado chegar na Bronze.

Se uma checagem falha, a transação é desfeita e a Bronze continua com a
última versão válida: dado ruim não contamina as camadas seguintes.
"""

from __future__ import annotations

from dataclasses import dataclass

from psycopg2 import sql


class DataQualityError(RuntimeError):
    """Lote rejeitado por violar uma regra de qualidade."""


@dataclass(frozen=True)
class BatchProfile:
    rows: int
    null_keys: int
    duplicated_keys: int


def check_row_count(rows: int, min_rows: int, incremental: bool, label: str) -> None:
    """Carga completa abaixo do mínimo indica extração quebrada ou origem truncada."""
    if not incremental and rows < min_rows:
        raise DataQualityError(f"{label}: {rows} linhas extraídas, mínimo esperado {min_rows}")


def check_profile(profile: BatchProfile, label: str) -> None:
    errors = []
    if profile.null_keys:
        errors.append(f"{profile.null_keys} linhas com chave primária nula")
    if profile.duplicated_keys:
        errors.append(f"{profile.duplicated_keys} chaves primárias duplicadas no lote")
    if errors:
        raise DataQualityError(f"{label}: " + "; ".join(errors))


def profile_query(table: sql.Composable, primary_key: tuple[str, ...]) -> sql.Composed:
    """Uma única passada na staging calcula volume, chaves nulas e duplicadas."""
    keys = sql.SQL(", ").join(map(sql.Identifier, primary_key))
    any_null = sql.SQL(" OR ").join(sql.SQL("{} IS NULL").format(sql.Identifier(c)) for c in primary_key)
    return sql.SQL(
        """
        SELECT count(*),
               count(*) FILTER (WHERE {any_null}),
               count(*) FILTER (WHERE NOT ({any_null}))
                 - count(DISTINCT ({keys})) FILTER (WHERE NOT ({any_null}))
        FROM {table}
        """
    ).format(any_null=any_null, keys=keys, table=table)
