FROM python:3.11-slim

# System basics
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl tini build-essential sqlite3 \
 && rm -rf /var/lib/apt/lists/*

# Python deps (pin a recent duckdb)
RUN pip install --no-cache-dir duckdb==1.0.0 pyarrow==17.0.0 pandas==2.2.2

# DuckDB CLI via pip (provides `duckdb` binary entrypoint through python -m duckdb, but add a shim)
RUN printf '#!/usr/bin/env bash\npython -m duckdb "$@"\n' > /usr/local/bin/duckdb && chmod +x /usr/local/bin/duckdb

WORKDIR /app
ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["bash","-lc","sleep infinity"]
