## Overview

`run_cubes.py` refreshes MicroStrategy OLAP cubes in parallel and logs each
execution in PostgreSQL. The included `schema.sql` creates the minimal tables
required for scheduling and auditing runs.

## Requirements

* Python 3.9+
* MicroStrategy credentials with permission to refresh the target cubes
* PostgreSQL database for metadata and logging

Install the runtime dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt  # equivalent to installing mstrio-py & psycopg2-binary
```

## Database setup

1. Create the schema and tables:

   ```bash
   psql "host=... dbname=... user=... password=..." -f schema.sql
   ```

2. Populate the cube catalog (example):

   ```bash
   psql ... -c "INSERT INTO mg_flow.mstr_cube_list(guid, cube_name, is_active, cube_execution_order) VALUES
   ('ABCDEF1234567890ABCDEF1234567890','Sales Cube',1,10),
   ('1234567890ABCDEF1234567890ABCDEF','Finance Cube',1,20)
   ON CONFLICT (guid) DO NOTHING;"
   ```

## Configuration

`run_cubes.py` reads configuration from environment variables. Export them in
your shell (or load via a `.env` helper):

```bash
export MSTR_BASE_URL="https://your-mstr-host/MicroStrategyLibrary/api"
export MSTR_USERNAME="administrator"
export MSTR_PASSWORD="********"
export MSTR_PROJECT="YourProject"
export MSTR_LOGIN_MODE=1           # optional; defaults to 1

export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_NAME=analytics
export DB_USER=postgres
export DB_PASS=postgres

export MAX_WORKERS=4               # optional
export WAIT_TIMEOUT_SEC=10800      # optional
export POLL_INTERVAL_SEC=5         # optional

# Email is optional. If any of these are missing, notifications are skipped.
export MAIL_FROM="bot@example.com"
export MAIL_TO="ops@example.com,bi@example.com"
export SMTP_HOST="smtp.example.com"
export SMTP_PORT=25
```

## Usage

Preview the run list without executing refreshes:

```bash
python run_cubes.py --all --dry-run
```

Refresh all active cubes:

```bash
python run_cubes.py --all
```

Refresh only cubes that have not logged a success today:

```bash
python run_cubes.py --missing-today
```

Additional options:

* `--max-workers N` — override parallel worker count.
* `--dry-run` — output the planned run order and exit.

The script prints a JSON summary to stdout and, if email is configured, sends
start/finish notifications.
