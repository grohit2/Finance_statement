.PHONY: setup ingest silver categorize gold export shares pipeline dashboard-dev test docker-up docker-sh rules-init rules-export rules-check

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
	cd apps/react-d3 && npm install || true

ingest:
	. .venv/bin/activate && PYTHONPATH=. python -m jobs.ingest_raw --conf conf/pipeline.yaml

silver:
	. .venv/bin/activate && PYTHONPATH=. python -m jobs.bronze_to_silver --conf conf/pipeline.yaml

categorize:
	. .venv/bin/activate && PYTHONPATH=. python -m jobs.categorize --conf conf/pipeline.yaml

gold:
	. .venv/bin/activate && PYTHONPATH=. python -m jobs.silver_to_gold --conf conf/pipeline.yaml

export:
	. .venv/bin/activate && PYTHONPATH=. python -m jobs.export_csv --conf conf/pipeline.yaml

shares:
	. .venv/bin/activate && python jobs/build_shares.py --profiles conf/profiles.yaml --out data/exports/csv/share_index.csv

pipeline: ingest silver categorize gold export

dashboard-dev:
	cd apps/react-d3 && npm run dev

test:
	. .venv/bin/activate && pytest -q

# --- Docker + DuckDB rules helpers ---

docker-up:
	cd ops/docker && docker compose up -d --build

docker-sh:
	cd ops/docker && docker compose exec rules bash

# Initialize DuckDB and seed rules
rules-init:
	cd ops/docker && docker compose exec rules bash -lc "duckdb conf/sql/rules.duckdb < conf/sql/init_rules.sql && echo '✅ rules.db seeded'"

# Export Parquet bundle for Spark
rules-export:
	cd ops/docker && docker compose exec rules bash -lc "python ops/scripts/export_rules_to_parquet.py"

# Validate NDJSON rules
rules-check:
	. .venv/bin/activate && PYTHONPATH=. python ops/scripts/rules_lint.py conf/rules
