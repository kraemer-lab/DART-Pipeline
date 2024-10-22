tests: lint install
	uv run pytest tests

install-uv:
	curl -LsSf https://astral.sh/uv/0.4.10/install.sh | sh

install:
	uv sync --all-extras --dev

lint:
	uvx ruff check src
	uvx ruff check tests

.PHONY: install-uv install lint tests
