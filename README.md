# mstr_run_cubes
# 1) Install deps
python -m venv .venv && . .venv/bin/activate
pip install --upgrade pip
pip install mstrio-py psycopg2-binary

# 2) Create DB schema
psql "host=... dbname=... user=... password=..." -f schema.sql

# 3) Seed cubes (example)
psql ... -c "INSERT INTO mg_flow.mstr_cube_list(guid,cube_name,is_active,cube_execution_order)
VALUES
('ABCDEF1234567890ABCDEF1234567890','Sales Cube',1,10),
('1234567890ABCDEF1234567890ABCDEF','Finance Cube',1,20)
ON CONFLICT (guid) DO NOTHING;"

# 4) Export env (or use a .env loader in your shell)
export MSTR_BASE_URL="https://your-mstr-host/MicroStrategyLibrary/api"
export MSTR_USERNAME="administrator"
export MSTR_PASSWORD="********"
export MSTR_PROJECT="YourProject"
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_NAME=analytics
export DB_USER=postgres
export DB_PASS=postgres
export MAIL_FROM="bot@example.com"
export MAIL_TO="ops@example.com,bi@example.com"
export SMTP_HOST="smtp.example.com"
export SMTP_PORT=25

# 5) Dry-run
python run_cubes.py --all --dry-run

# 6) Run all active cubes
python run_cubes.py --all

# 7) Or run only cubes without a success log today
python run_cubes.py --missing-today