#!/bin/bash

# Script to run the InfluxDB ingestion tests

cd "$(dirname "$0")"

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
  echo "Error: Docker is not running. Please start Docker and try again."
  exit 1
fi

# Install test dependencies
if [ ! -f ".venv/bin/activate" ]; then
  echo "Creating virtual environment and installing dependencies..."
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
else
  source .venv/bin/activate
fi

# Clean up existing containers
echo "Cleaning up any existing test containers..."
docker compose -f docker-compose.yml down -v 2>/dev/null

echo "Running tests..."
pytest -v

echo "Tests completed!"
