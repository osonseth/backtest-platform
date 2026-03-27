#!/bin/bash

cd "$(dirname "$0")/.."

if command -v podman &> /dev/null; then
    mkdir -p ~/goinfre/backtesting_data
else
    mkdir -p ~/backtesting_data
fi

docker compose --env-file .env -f infra/docker-compose.yml up -d --build