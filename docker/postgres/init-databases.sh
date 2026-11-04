#!/usr/bin/env bash
set -euo pipefail

export PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}"

for attempt in {1..30}; do
  if pg_isready \
    --host postgres \
    --username "${POSTGRES_SUPERUSER}" \
    --dbname postgres >/dev/null 2>&1; then
    break
  fi

  if [[ "${attempt}" -eq 30 ]]; then
    echo "PostgreSQL did not become ready before the bootstrap timeout" >&2
    exit 1
  fi

  sleep 2
done

psql \
  --host postgres \
  --username "${POSTGRES_SUPERUSER}" \
  --dbname postgres \
  --output /dev/null \
  --set ON_ERROR_STOP=1 \
  --set airflow_user="${AIRFLOW_DB_USER}" \
  --set airflow_password="${AIRFLOW_DB_PASSWORD}" \
  --set airflow_db="${AIRFLOW_DB_NAME}" \
  --set pncp_user="${PNCP_DB_USER}" \
  --set pncp_password="${PNCP_DB_PASSWORD}" \
  --set pncp_db="${PNCP_DB_NAME}" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'airflow_user', :'airflow_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'airflow_user');
\gexec

SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'airflow_user', :'airflow_password');
\gexec

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'pncp_user', :'pncp_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'pncp_user');
\gexec

SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'pncp_user', :'pncp_password');
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', :'airflow_db', :'airflow_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'airflow_db');
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', :'pncp_db', :'pncp_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'pncp_db');
\gexec
SQL

for migration in /migrations/*.sql; do
  psql \
    --host postgres \
    --username "${POSTGRES_SUPERUSER}" \
    --dbname "${PNCP_DB_NAME}" \
    --set ON_ERROR_STOP=1 \
    --set app_user="${PNCP_DB_USER}" \
    --file "${migration}"
done
