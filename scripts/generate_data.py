"""Gera dados fictícios para os três sistemas de origem.

Simula o cenário pós-fusão: três provedores de internet, cada um com um
sistema próprio, convenções de nomes diferentes e problemas de qualidade
típicos de dados operacionais. Nenhum dado aqui é real.

    Alfa Fibra  - sistema moderno em português (snake_case, tipos corretos)
    Beta Net    - sistema em inglês, valores em centavos, status numérico
    Gama Telecom - sistema legado: prefixos tb_/cod_/dt_, datas em texto,
                   flags 'S'/'N' e códigos de uma letra

Uso:
    python scripts/generate_data.py            # recria as três origens
    python scripts/generate_data.py --increment  # simula um novo dia de operação
"""

from __future__ import annotations

import argparse
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import psycopg2
from faker import Faker

SEED = 42
DEFAULT_DSN = "postgresql://postgres@localhost:5432/{db}"  # senha via PGPASSWORD

PLANOS = [
    ("Fibra 300 Mega", 99.90),
    ("Fibra 500 Mega", 119.90),
    ("Fibra 700 Mega", 139.90),
    ("Fibra 1 Giga", 179.90),
    ("Empresarial 1 Giga", 349.90),
]


@dataclass
class Pessoa:
    nome: str
    cpf: str
    email: str | None
    cidade: str
    uf: str


def dsn(db: str) -> str:
    return os.getenv(f"{db.upper()}_DSN", DEFAULT_DSN.format(db=db))


def reference_date() -> date:
    raw = os.getenv("REFERENCE_DATE")
    return date.fromisoformat(raw) if raw else date.today()


def random_dt(fake: Faker, start: date, end: date) -> datetime:
    return fake.date_time_between_dates(
        datetime.combine(start, datetime.min.time()),
        datetime.combine(end, datetime.min.time()),
    )


def gerar_pessoas(fake: Faker, n: int) -> list[Pessoa]:
    pessoas = []
    for _ in range(n):
        email = fake.email() if random.random() > 0.08 else None
        pessoas.append(
            Pessoa(
                nome=fake.name(),
                cpf=fake.cpf(),
                email=email,
                cidade=fake.city(),
                uf=fake.estado_sigla(),
            )
        )
    return pessoas


def gerar_contratos(fake: Faker, n_clientes: int, hoje: date) -> list[dict]:
    """Contratos genéricos: depois cada origem converte para o próprio formato."""
    contratos = []
    for cliente_idx in range(n_clientes):
        for _ in range(1 if random.random() < 0.8 else 2):
            plano, valor = random.choice(PLANOS)
            ativacao = random_dt(fake, hoje - timedelta(days=900), hoje - timedelta(days=1))
            r = random.random()
            if r < 0.22:
                status = "cancelado"
                cancelamento = random_dt(fake, ativacao.date() + timedelta(days=1), hoje)
            elif r < 0.27:
                status, cancelamento = "suspenso", None
            else:
                status, cancelamento = "ativo", None
            atualizado = cancelamento or random_dt(fake, ativacao.date(), hoje)
            contratos.append(
                {
                    "cliente_idx": cliente_idx,
                    "plano": plano,
                    "valor": valor,
                    "status": status,
                    "ativacao": ativacao,
                    "cancelamento": cancelamento,
                    "atualizado": max(atualizado, ativacao),
                }
            )
    return contratos


def gerar_ordens(fake: Faker, contratos: list[dict], hoje: date) -> list[dict]:
    agora = datetime.combine(hoje, datetime.min.time())
    ordens = []
    for idx, c in enumerate(contratos):
        tipos = ["instalacao"]
        tipos += ["reparo"] * random.choice([0, 0, 1, 1, 2, 3])
        if c["status"] == "cancelado":
            tipos.append("retirada")
        for tipo in tipos:
            if tipo == "instalacao":
                aberta = c["ativacao"] - timedelta(hours=random.randint(24, 240))
            elif tipo == "retirada":
                aberta = min(c["cancelamento"] + timedelta(hours=random.randint(1, 48)), agora)
            else:
                aberta = random_dt(fake, c["ativacao"].date(), hoje)
            sla_h = {"instalacao": 72, "reparo": 24, "retirada": 120}[tipo]
            fechada = aberta + timedelta(hours=random.expovariate(1 / (sla_h * 0.7)))
            if fechada > agora or random.random() < 0.03:
                fechada = None  # OS ainda em aberto
            ordens.append(
                {
                    "contrato_idx": idx,
                    "tipo": tipo,
                    "aberta": aberta,
                    "fechada": fechada,
                    "atualizado": fechada or aberta,
                }
            )
    return ordens


# --------------------------------------------------------------------------
# Escrita em cada sistema de origem
# --------------------------------------------------------------------------

DDL = {
    "alfa": """
        DROP TABLE IF EXISTS ordem_servico, contrato, cliente;
        CREATE TABLE cliente (
            id integer PRIMARY KEY, nome text, cpf_cnpj text, email text,
            cidade text, uf char(2), criado_em timestamp);
        CREATE TABLE contrato (
            id integer PRIMARY KEY, cliente_id integer REFERENCES cliente,
            plano text, valor_mensal numeric(10,2), status text,
            data_ativacao date, data_cancelamento date, atualizado_em timestamp);
        CREATE TABLE ordem_servico (
            id integer PRIMARY KEY, contrato_id integer REFERENCES contrato,
            tipo text, aberta_em timestamp, fechada_em timestamp,
            atualizado_em timestamp);
    """,
    "beta": """
        DROP TABLE IF EXISTS tickets, subscriptions, customers;
        CREATE TABLE customers (
            customer_id bigint PRIMARY KEY, full_name text, document text,
            email text, city text, state text, created_at timestamptz);
        CREATE TABLE subscriptions (
            subscription_id bigint PRIMARY KEY, customer_id bigint REFERENCES customers,
            plan_name text, monthly_fee_cents integer, status_code smallint,
            activated_on date, canceled_on date, updated_at timestamptz);
        CREATE TABLE tickets (
            ticket_id bigint PRIMARY KEY, subscription_id bigint REFERENCES subscriptions,
            ticket_type text, opened_at timestamptz, closed_at timestamptz,
            updated_at timestamptz);
    """,
    "gama": """
        DROP TABLE IF EXISTS tb_os, tb_contrato, tb_cliente;
        CREATE TABLE tb_cliente (
            cod_cliente varchar(10) PRIMARY KEY, nome_razao varchar(120),
            num_doc varchar(20), email varchar(120), municipio varchar(80),
            uf varchar(2), dt_cadastro varchar(10));
        CREATE TABLE tb_contrato (
            cod_contrato varchar(10) PRIMARY KEY, cod_cliente varchar(10),
            desc_plano varchar(60), vlr_mensal varchar(12), fl_ativo char(1),
            fl_suspenso char(1), dt_ativacao varchar(10), dt_cancelamento varchar(10),
            dt_alteracao timestamp);
        CREATE TABLE tb_os (
            cod_os varchar(12) PRIMARY KEY, cod_contrato varchar(10), tp_os char(1),
            dt_abertura timestamp, dt_fechamento timestamp, dt_alteracao timestamp);
    """,
}


def escrever_alfa(cur, pessoas, contratos, ordens):
    for i, p in enumerate(pessoas, start=1):
        criado = contratos_por_cliente_min(contratos, i - 1)
        cur.execute(
            "INSERT INTO cliente VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (i, p.nome, p.cpf, p.email, p.cidade, p.uf, criado),
        )
    for i, c in enumerate(contratos, start=1):
        cur.execute(
            "INSERT INTO contrato VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                i,
                c["cliente_idx"] + 1,
                c["plano"],
                c["valor"],
                c["status"].upper(),
                c["ativacao"].date(),
                c["cancelamento"] and c["cancelamento"].date(),
                c["atualizado"],
            ),
        )
    for i, o in enumerate(ordens, start=1):
        cur.execute(
            "INSERT INTO ordem_servico VALUES (%s,%s,%s,%s,%s,%s)",
            (i, o["contrato_idx"] + 1, o["tipo"].upper(), o["aberta"], o["fechada"], o["atualizado"]),
        )


def escrever_beta(cur, pessoas, contratos, ordens):
    status_code = {"ativo": 1, "suspenso": 2, "cancelado": 3}
    ticket_type = {"instalacao": "install", "reparo": "repair", "retirada": "removal"}
    base = 900_000
    for i, p in enumerate(pessoas, start=1):
        doc = "".join(ch for ch in p.cpf if ch.isdigit())
        email = f"  {p.email.upper()} " if p.email and random.random() < 0.3 else p.email
        cur.execute(
            "INSERT INTO customers VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (base + i, p.nome, doc, email, p.cidade, p.uf, contratos_por_cliente_min(contratos, i - 1)),
        )
    for i, c in enumerate(contratos, start=1):
        cur.execute(
            "INSERT INTO subscriptions VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                base + i,
                base + c["cliente_idx"] + 1,
                c["plano"].lower(),
                round(c["valor"] * 100),
                status_code[c["status"]],
                c["ativacao"].date(),
                c["cancelamento"] and c["cancelamento"].date(),
                c["atualizado"],
            ),
        )
    for i, o in enumerate(ordens, start=1):
        cur.execute(
            "INSERT INTO tickets VALUES (%s,%s,%s,%s,%s,%s)",
            (
                base + i,
                base + o["contrato_idx"] + 1,
                ticket_type[o["tipo"]],
                o["aberta"],
                o["fechada"],
                o["atualizado"],
            ),
        )


def escrever_gama(cur, pessoas, contratos, ordens):
    tp = {"instalacao": "I", "reparo": "R", "retirada": "D"}
    br = lambda d: d.strftime("%d/%m/%Y") if d else None  # noqa: E731
    for i, p in enumerate(pessoas, start=1):
        cur.execute(
            "INSERT INTO tb_cliente VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                f"C{i:06d}",
                p.nome.upper(),
                p.cpf,
                p.email,
                p.cidade.upper(),
                p.uf.lower() if random.random() < 0.2 else p.uf,
                br(contratos_por_cliente_min(contratos, i - 1)),
            ),
        )
    for i, c in enumerate(contratos, start=1):
        cur.execute(
            "INSERT INTO tb_contrato VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                f"K{i:06d}",
                f"C{c['cliente_idx'] + 1:06d}",
                c["plano"].upper(),
                f"{c['valor']:.2f}".replace(".", ","),
                "N" if c["status"] == "cancelado" else "S",
                "S" if c["status"] == "suspenso" else "N",
                br(c["ativacao"]),
                br(c["cancelamento"]),
                c["atualizado"],
            ),
        )
    for i, o in enumerate(ordens, start=1):
        cur.execute(
            "INSERT INTO tb_os VALUES (%s,%s,%s,%s,%s,%s)",
            (f"OS{i:08d}", f"K{o['contrato_idx'] + 1:06d}", tp[o["tipo"]], o["aberta"], o["fechada"], o["atualizado"]),
        )


_PRIMEIRA_ATIVACAO: dict[int, dict[int, datetime]] = {}


def contratos_por_cliente_min(contratos: list[dict], cliente_idx: int) -> datetime | None:
    """Data de cadastro do cliente: alguns dias antes do primeiro contrato."""
    indice = _PRIMEIRA_ATIVACAO.get(id(contratos))
    if indice is None:
        indice = {}
        for c in contratos:
            atual = indice.get(c["cliente_idx"])
            indice[c["cliente_idx"]] = min(atual, c["ativacao"]) if atual else c["ativacao"]
        _PRIMEIRA_ATIVACAO[id(contratos)] = indice
    primeira = indice.get(cliente_idx)
    return primeira - timedelta(days=random.randint(1, 15)) if primeira else None


ESCRITORES = {"alfa": escrever_alfa, "beta": escrever_beta, "gama": escrever_gama}
VOLUMES = {"alfa": 3000, "beta": 2000, "gama": 1500}


def gerar_tudo() -> None:
    random.seed(SEED)
    fake = Faker("pt_BR")
    Faker.seed(SEED)
    hoje = reference_date()
    pessoas_alfa = gerar_pessoas(fake, VOLUMES["alfa"])
    # ~10% dos clientes da Beta também eram clientes da Alfa (mesmo CPF):
    # é isso que a Gold resolve ao unificar a pessoa entre as empresas.
    compartilhados = random.sample(pessoas_alfa, VOLUMES["beta"] // 10)
    pessoas = {
        "alfa": pessoas_alfa,
        "beta": compartilhados + gerar_pessoas(fake, VOLUMES["beta"] - len(compartilhados)),
        "gama": gerar_pessoas(fake, VOLUMES["gama"]),
    }
    for origem, lista in pessoas.items():
        contratos = gerar_contratos(fake, len(lista), hoje)
        ordens = gerar_ordens(fake, contratos, hoje)
        with psycopg2.connect(dsn(origem)) as conn, conn.cursor() as cur:
            cur.execute(DDL[origem])
            ESCRITORES[origem](cur, lista, contratos, ordens)
        print(f"{origem}: {len(lista)} clientes, {len(contratos)} contratos, {len(ordens)} OS")


def simular_novo_dia() -> None:
    """Altera registros recentes para demonstrar a carga incremental."""
    agora = datetime.now()
    updates = {
        "alfa": [
            "UPDATE contrato SET status='CANCELADO', data_cancelamento=%(d)s, atualizado_em=%(t)s "
            "WHERE id IN (SELECT id FROM contrato WHERE status='ATIVO' ORDER BY random() LIMIT 15)",
            "UPDATE ordem_servico SET fechada_em=%(t)s, atualizado_em=%(t)s WHERE fechada_em IS NULL",
        ],
        "beta": [
            "UPDATE subscriptions SET status_code=3, canceled_on=%(d)s, updated_at=%(t)s "
            "WHERE subscription_id IN (SELECT subscription_id FROM subscriptions "
            "WHERE status_code=1 ORDER BY random() LIMIT 10)",
        ],
        "gama": [
            "UPDATE tb_contrato SET fl_ativo='N', dt_cancelamento=to_char(%(d)s::date,'DD/MM/YYYY'), "
            "dt_alteracao=%(t)s WHERE cod_contrato IN (SELECT cod_contrato FROM tb_contrato "
            "WHERE fl_ativo='S' ORDER BY random() LIMIT 8)",
        ],
    }
    for origem, comandos in updates.items():
        with psycopg2.connect(dsn(origem)) as conn, conn.cursor() as cur:
            for sql in comandos:
                cur.execute(sql, {"d": agora.date(), "t": agora})
                print(f"{origem}: {cur.rowcount} linhas alteradas")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--increment", action="store_true", help="simula um novo dia de operação")
    args = parser.parse_args()
    simular_novo_dia() if args.increment else gerar_tudo()
