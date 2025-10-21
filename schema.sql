-- Minimal, generic schema to support run_cubes.py

CREATE SCHEMA IF NOT EXISTS mg_flow;

-- Master list of cubes to drive execution
CREATE TABLE IF NOT EXISTS mg_flow.mstr_cube_list (
    guid                 TEXT PRIMARY KEY,              -- MicroStrategy object id
    cube_name            TEXT NOT NULL,                 -- human-readable name
    is_active            SMALLINT NOT NULL DEFAULT 1,   -- 1=active, 0=inactive
    cube_execution_order INTEGER,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS mstr_cube_list_active_idx
    ON mg_flow.mstr_cube_list (is_active, cube_execution_order NULLS LAST);

-- Per-run logging
CREATE TABLE IF NOT EXISTS mg_flow.mstr_cube_list_log (
    flow_date    DATE NOT NULL DEFAULT CURRENT_DATE,
    flow_run_no  INTEGER NOT NULL,                      -- daily run sequence (1..N)
    guid         TEXT NOT NULL REFERENCES mg_flow.mstr_cube_list(guid) ON DELETE CASCADE,
    start_ts     TIMESTAMPTZ NOT NULL,
    end_ts       TIMESTAMPTZ,
    status       TEXT NOT NULL CHECK (status IN ('running','success','fail')),
    fail_log     TEXT,
    duration_sec NUMERIC(18,2),
    row_count    BIGINT,
    data_gb      NUMERIC(18,6),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast lookups
CREATE INDEX IF NOT EXISTS mstr_cube_list_log_date_idx
    ON mg_flow.mstr_cube_list_log (flow_date, status);

CREATE INDEX IF NOT EXISTS mstr_cube_list_log_guid_idx
    ON mg_flow.mstr_cube_list_log (guid, flow_date);

-- Prevent exact duplicates for a given run
CREATE UNIQUE INDEX IF NOT EXISTS mstr_cube_list_log_uniq
    ON mg_flow.mstr_cube_list_log (flow_date, flow_run_no, guid, start_ts);