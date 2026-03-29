.PHONY: help setup test lint bronze silver gold pipeline dqa status

help:
	@echo ""
	@echo "DQA Real Estate — Kommandos"
	@echo "─────────────────────────────────────────────────────"
	@echo "  make setup          Abhängigkeiten installieren"
	@echo "  make test           Alle Unit-Tests ausführen"
	@echo "  make lint           Code-Qualität prüfen"
	@echo ""
	@echo "  make connection     Verbindungen zu Databricks & GWR prüfen"
	@echo "  make bronze         GWR-Daten inkrementell abrufen"
	@echo "  make bronze-full    Vollabzug (ignoriert Watermark)"
	@echo "  make silver         SCD2-Verarbeitung: Bronze → Silver"
	@echo "  make gold           Gold-Views aktualisieren"
	@echo "  make pipeline       bronze + silver + gold in einem Schritt"
	@echo "  make status         Datenbestand-Übersicht"
	@echo ""
	@echo "  make dqa FILE=meine_daten.csv   DQA-Checks starten"
	@echo "─────────────────────────────────────────────────────"

setup:
	pip install -r requirements.txt
	pre-commit install
	cp -n .env.example .env || true
	@echo "Bitte .env mit deinen Databricks-Credentials befüllen."

test:
	pytest tests/ -v --cov=src --cov-report=term-missing

test-fast:
	pytest tests/ -v -x

lint:
	ruff check src/ tests/
	black --check src/ tests/

format:
	black src/ tests/
	isort src/ tests/

connection:
	python main.py test-connection

bronze:
	python main.py bronze

bronze-full:
	python main.py bronze --full-load

bronze-kanton:
	@if [ -z "$(KANTON)" ]; then echo "Verwendung: make bronze-kanton KANTON=AG"; exit 1; fi
	python main.py bronze --kanton $(KANTON)

silver:
	python main.py silver

gold:
	python main.py gold

pipeline:
	python main.py pipeline

pipeline-full:
	python main.py pipeline --full-load

status:
	python main.py status

dqa:
	@if [ -z "$(FILE)" ]; then echo "Verwendung: make dqa FILE=meine_daten.csv"; exit 1; fi
	python main.py dqa $(FILE) --output results/dqa_$(shell date +%Y%m%d_%H%M%S).json

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf .pytest_cache/ .coverage htmlcov/
