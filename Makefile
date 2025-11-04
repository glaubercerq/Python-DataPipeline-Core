# Makefile para automação de tarefas do projeto ETL

.PHONY: help install test clean run setup lint format

help:  ## Mostra esta mensagem de ajuda
	@echo "Comandos disponíveis:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:  ## Instala as dependências do projeto
	pip install -r requirements.txt

test:  ## Executa os testes
	pytest tests/ -v

run:  ## Executa o pipeline ETL
	python scripts/run_pipeline.py

setup:  ## Configura o banco de dados
	python scripts/setup_db.py

clean:  ## Limpa arquivos temporários
	python scripts/clean_data.py

lint:  ## Verifica qualidade do código (requer flake8)
	flake8 etl_scripts/ --max-line-length=120

format:  ## Formata o código (requer black)
	black etl_scripts/ tests/ scripts/
