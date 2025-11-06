#!/bin/bash
# Start DB + Adminer, wait for DB, load data, run sample queries

set -euo pipefail

echo "[1/4] Start DB + Adminer…"
docker compose up -d db adminer      # start both services

echo "[2/4] Wait for DB healthy…"
until [ "$(docker inspect -f '{{.State.Health.Status}}' $(docker compose ps -q db))" = "healthy" ]; do
  sleep 1
  echo -n "."
done
echo " DB is healthy."

echo "[3/4] Load data…"
docker compose run --rm app python load_data.py --host db --dbname transit --user transit --password transit123

echo "[4/4] Run sample queries…"
docker compose run --rm app python queries.py --host db --dbname transit --user transit --password transit123 --query Q1
docker compose run --rm app python queries.py --host db --dbname transit --user transit --password transit123 --query Q2

echo "All good. Open Adminer at: http://localhost:8080"
