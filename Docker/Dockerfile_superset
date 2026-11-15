FROM apache/superset:6.1.0

# The official image ships without database drivers; add the PostgreSQL one.
USER root
RUN uv pip install --no-cache psycopg2-binary
USER superset
