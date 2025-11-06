#!/bin/bash
# Run all queries (Q1..Q10) to validate logic/perf
set -euo pipefail

./build.sh
./run.sh

echo
echo "Run all queries…"
for i in $(seq 1 10); do
  docker-compose run --rm app python queries.py --host db --dbname transit --user transit --password transit123 --query Q$i >/dev/null
  echo "Q$i OK"
done

# Optional: tear down containers (keep volume for faster reruns)
# docker-compose down
