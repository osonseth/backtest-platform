#!/bin/bash

cd "$(dirname "$0")/.."

echo "Stopping container..."
docker compose --env-file .env -f infra/docker-compose.yml down

if command -v podman &> /dev/null; then
    echo "Deleting data..."
    docker unshare rm -rf ~/goinfre/backtesting_data
    echo "Recreating data folder..."
    mkdir -p ~/goinfre/backtesting_data
else
    echo "Deleting data..."
    rm -rf ~/backtesting_data
    echo "Recreating data folder..."
    mkdir -p ~/backtesting_data
fi

echo "Starting container..."
docker compose --env-file .env -f infra/docker-compose.yml up -d --build

echo "Done."