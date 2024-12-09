tests: lint install
	uv run pytest tests

install-uv:
	curl -LsSf https://astral.sh/uv/0.4.10/install.sh | sh

install:
	uv sync --all-extras --dev

lint:
	uvx ruff check src
	uvx ruff check tests

docs:
	uv run -m sphinx -T -b html -d docs/_build/doctrees -D language=en docs html

.PHONY: install-uv install lint tests docs
