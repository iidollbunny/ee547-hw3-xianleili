# Dockerfile
# Small Python image with psql client for troubleshooting
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code and data
COPY schema.sql load_data.py queries.py ./
COPY data ./data

# Default just runs loader (you can override CMD in docker compose/run)
CMD ["python", "load_data.py", "--host", "db", "--dbname", "transit", "--user", "transit", "--password", "transit123"]
