"""Leitura e validação da declaração da camada Bronze (config/sources.yaml)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "sources.yaml"


class ConfigError(ValueError):
    """Erro de declaração no sources.yaml."""


@dataclass(frozen=True)
class Entity:
    name: str
    table: str
    primary_key: tuple[str, ...]
    incremental_by: str | None = None
    min_rows: int = 0

    @property
    def is_incremental(self) -> bool:
        return self.incremental_by is not None


@dataclass(frozen=True)
class Source:
    name: str
    conn_id: str
    schedule: str
    retries: int
    retry_delay_minutes: int
    description: str = ""
    entities: tuple[Entity, ...] = field(default_factory=tuple)

    @property
    def bronze_schema(self) -> str:
        return f"bronze_{self.name}"


def dataset_uri(source: str, entity: str) -> str:
    """URI do Airflow Dataset emitido quando uma entidade da Bronze é atualizada."""
    return f"medallion://bronze/{source}/{entity}"


def _identifier(value: object, where: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.match(value):
        raise ConfigError(f"{where}: '{value}' não é um identificador válido")
    return value


def _parse_entity(raw: dict, where: str) -> Entity:
    pk = raw.get("primary_key")
    if not pk:
        raise ConfigError(f"{where}: primary_key é obrigatória")
    incremental = raw.get("incremental_by")
    min_rows = raw.get("min_rows", 0)
    if not isinstance(min_rows, int) or min_rows < 0:
        raise ConfigError(f"{where}: min_rows deve ser um inteiro >= 0")
    return Entity(
        name=_identifier(raw.get("name"), f"{where}.name"),
        table=_identifier(raw.get("table"), f"{where}.table"),
        primary_key=tuple(_identifier(c, f"{where}.primary_key") for c in pk),
        incremental_by=_identifier(incremental, f"{where}.incremental_by") if incremental else None,
        min_rows=min_rows,
    )


def load_sources(path: Path | str = DEFAULT_CONFIG) -> list[Source]:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    defaults = data.get("defaults", {})
    sources = []
    for name, raw in (data.get("sources") or {}).items():
        where = f"sources.{name}"
        entities = [_parse_entity(e, f"{where}.entities[{i}]") for i, e in enumerate(raw.get("entities", []))]
        if not entities:
            raise ConfigError(f"{where}: declare ao menos uma entidade")
        names = [e.name for e in entities]
        if len(names) != len(set(names)):
            raise ConfigError(f"{where}: nomes de entidade duplicados")
        merged = {**defaults, **raw}
        schedule = merged.get("schedule")
        if not schedule:
            raise ConfigError(f"{where}: schedule ausente e sem default")
        sources.append(
            Source(
                name=_identifier(name, where),
                conn_id=_identifier(raw.get("conn_id"), f"{where}.conn_id"),
                schedule=schedule,
                retries=int(merged.get("retries", 0)),
                retry_delay_minutes=int(merged.get("retry_delay_minutes", 5)),
                description=raw.get("description", ""),
                entities=tuple(entities),
            )
        )
    if not sources:
        raise ConfigError("nenhuma origem declarada")
    return sources
