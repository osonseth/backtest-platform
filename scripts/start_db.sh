#!/bin/bash

cd "$(dirname "$0")/.."
mkdir -p ~/goinfre/backtesting_data
docker compose -f infra/docker-compose.yml up -d --build