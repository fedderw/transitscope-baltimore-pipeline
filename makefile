format:
	isort . && black . -l 79
pre-commit:
	pre-commit run --all
test:
	pytest .
