FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Esta MISMA imagen la usan scheduler, los 3 workers y el runner
# (ver docker-compose.yml), asi se garantiza que dask/distributed/
# pandas/python sean identicos en cliente y cluster.
# scripts/ y orchestration/ se montan como volumenes solo en 'runner'.
ENV PYTHONPATH=/app/scripts

# Se queda vivo para que entres con: docker compose exec runner bash
CMD ["sleep", "infinity"]
