format:
	isort . && black . -l 79
pre-commit:
	 pre-commit run --all
test:
	pytest -v --cov=src --cov-report=term-missing
