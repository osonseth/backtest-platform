#!/bin/bash

cd "$(dirname "$0")/.."

echo "Stopping container..."
docker compose -f infra/docker-compose.yml down

echo "Deleting data..."
docker unshare rm -rf ~/goinfre/backtesting_data

echo "Recreating data folder..."
mkdir -p ~/goinfre/backtesting_data

echo "Starting container..."
docker compose -f infra/docker-compose.yml up -d --build

echo "Done."
