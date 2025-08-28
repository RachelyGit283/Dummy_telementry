# Makefile
.PHONY: help install test clean build run-example lint format

help:
	@echo "Available commands:"
	@echo "  make install      - Install the package in development mode"
	@echo "  make test        - Run all tests"
	@echo "  make test-fast   - Run tests excluding slow ones"
	@echo "  make test-cov    - Run tests with coverage report"
	@echo "  make clean       - Remove generated files"
	@echo "  make lint        - Run linting checks"
	@echo "  make format      - Format code with black"
	@echo "  make run-example - Run example generation"
	@echo "  make build       - Build distribution packages"

install:
	pip install -e .
	pip install -r requirements-dev.txt

test:
	pytest tests/ -v

test-fast:
	pytest tests/ -v -m "not slow"

test-cov:
	pytest tests/ --cov=telemetry_generator --cov-report=html --cov-report=term

clean:
	rm -rf build/ dist/ *.egg-info
	rm -rf data/
	rm -rf htmlcov/ .coverage
	rm -rf .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

lint:
	flake8 telemetry_generator/ tests/
	mypy telemetry_generator/ --ignore-missing-imports

format:
	black telemetry_generator/ tests/

run-example:
	@echo "Generating example telemetry data..."
	telegen generate \
		--schema example_schema.json \
		--rate 1000 \
		--duration 10 \
		--out-dir data/ \
		--format ndjson
	@echo "Generated files:"
	@ls -lh data/

build:
	python setup.py sdist bdist_wheel
