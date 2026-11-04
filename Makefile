.PHONY: up down reset novo-dia test lint dbt-build local

up:            ## sobe Postgres + Airflow e gera os dados fictícios
	@test -f .env || (cp .env.example .env && echo "Criado .env a partir do .env.example: troque as senhas.")
	docker compose up -d --build
	@echo "Airflow em http://localhost:8080 (usuário e senha definidos no .env)"

down:          ## para o ambiente
	docker compose down

reset:         ## apaga tudo, inclusive os dados
	docker compose down -v

novo-dia:      ## simula alterações nas origens para ver a carga incremental
	docker compose exec airflow-scheduler python /opt/airflow/scripts/generate_data.py --increment

lint:
	ruff check . && ruff format --check .

test:
	pytest -v

local:         ## pipeline completo sem Docker (requer Postgres local e PGPASSWORD/DW_PASSWORD)
	python scripts/generate_data.py
	python scripts/run_bronze.py
	cd dbt && dbt build --profiles-dir .
