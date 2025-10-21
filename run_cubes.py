#!/usr/bin/env python3
"""
Generic MicroStrategy OLAP Cube runner & logger

- No company-specific values; everything is configurable via ENV or CLI.
- Logs each cube run to PostgreSQL (schema below).
- Parallel refresh with job polling and basic metrics (row_count, data_gb).
- Email is optional; if SMTP env is missing, it is skipped.
- Output and logs use ASCII-safe text (no locale-specific characters).

Requirements:
  pip install mstrio-py psycopg2-binary

Env vars (samples):
  # MicroStrategy
  MSTR_BASE_URL=https://your-mstr-host/MicroStrategyLibrary/api
  MSTR_USERNAME=administrator
  MSTR_PASSWORD=********
  MSTR_PROJECT=YourProject
  MSTR_LOGIN_MODE=1  # 1=Standard, 8=LDAP

  # PostgreSQL (psycopg2)
  DB_HOST=127.0.0.1
  DB_PORT=5432
  DB_NAME=analytics
  DB_USER=postgres
  DB_PASS=postgres

  # Concurrency & polling
  MAX_WORKERS=4
  WAIT_TIMEOUT_SEC=10800
  POLL_INTERVAL_SEC=5

  # Email (optional; if not set -> disabled)
  SMTP_HOST=smtp.example.com
  SMTP_PORT=25
  MAIL_FROM=bot@example.com
  MAIL_TO=ops@example.com,bi@example.com
"""

import argparse
import json
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import EmailMessage
from typing import List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from mstrio.connection import Connection
from mstrio.project_objects.datasets.olap_cube import OlapCube
from mstrio.server.job_monitor import Job, JobStatus
from mstrio.project_objects.datasets.cube_cache import list_cube_caches

# -------------------- config helpers --------------------

def env_str(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    return v if v is not None else default

def env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default

# MicroStrategy
MSTR_BASE_URL   = env_str("MSTR_BASE_URL",   "")
MSTR_USERNAME   = env_str("MSTR_USERNAME",   "")
MSTR_PASSWORD   = env_str("MSTR_PASSWORD",   "")
MSTR_PROJECT    = env_str("MSTR_PROJECT",    "")
MSTR_LOGIN_MODE = env_int("MSTR_LOGIN_MODE", 1)

# PostgreSQL
DB_HOST = env_str("DB_HOST", "127.0.0.1")
DB_PORT = env_int("DB_PORT", 5432)
DB_NAME = env_str("DB_NAME", "analytics")
DB_USER = env_str("DB_USER", "postgres")
DB_PASS = env_str("DB_PASS", "postgres")

# Parallelism & polling
MAX_WORKERS      = env_int("MAX_WORKERS", 4)
WAIT_TIMEOUT_SEC = env_int("WAIT_TIMEOUT_SEC", 10800)
POLL_INTERVAL_SEC= env_int("POLL_INTERVAL_SEC", 5)

# Email (optional)
SMTP_HOST = env_str("SMTP_HOST", None)
SMTP_PORT = env_int("SMTP_PORT", 25)
MAIL_FROM = env_str("MAIL_FROM", None)
MAIL_TO   = [x.strip() for x in env_str("MAIL_TO", "") .split(",") if x.strip()]

# -------------------- db helpers --------------------

def db_connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
    )

def fetch_active_cubes_all() -> List[Tuple[str, int, str]]:
    """
    Return all active cubes, ordered by execution order then name.
    -> (guid, order, name)
    """
    sql = """
        SELECT guid,
               COALESCE(cube_execution_order, 999999) AS ord,
               COALESCE(NULLIF(TRIM(cube_name), ''), guid) AS cube_name
          FROM mg_flow.mstr_cube_list
         WHERE COALESCE(is_active, 1) = 1
         ORDER BY ord ASC, cube_name ASC
    """
    with db_connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        return [(r["guid"], int(r["ord"]), r["cube_name"]) for r in rows]

def fetch_cubes_without_success_today() -> List[Tuple[str, int, str]]:
    """
    Return active cubes which DO NOT have any 'success' record today.
    """
    sql = """
        SELECT l.guid,
               COALESCE(l.cube_execution_order, 999999) AS ord,
               COALESCE(NULLIF(TRIM(l.cube_name), ''), l.guid) AS cube_name
          FROM mg_flow.mstr_cube_list l
         WHERE COALESCE(l.is_active, 1) = 1
           AND NOT EXISTS (
                 SELECT 1
                   FROM mg_flow.mstr_cube_list_log lg
                  WHERE lg.flow_date = CURRENT_DATE
                    AND lg.guid = l.guid
                    AND lg.status = 'success'
           )
         ORDER BY ord ASC, cube_name ASC
    """
    with db_connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        return [(r["guid"], int(r["ord"]), r["cube_name"]) for r in rows]

def compute_today_flow_run_no() -> int:
    sql = "SELECT COALESCE(MAX(flow_run_no), 0) + 1 AS n FROM mg_flow.mstr_cube_list_log WHERE flow_date = CURRENT_DATE"
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]

def log_running(guid: str, run_no: int, start_ts: float):
    sql = """
        INSERT INTO mg_flow.mstr_cube_list_log
        (flow_date, flow_run_no, guid, start_ts, end_ts, status, fail_log, duration_sec, row_count, data_gb)
        VALUES (CURRENT_DATE, %s, %s, to_timestamp(%s), NULL, 'running', NULL, NULL, NULL, NULL)
    """
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(sql, (run_no, guid, start_ts))

def log_update_result(
    guid: str, run_no: int, start_ts: float, end_ts: float,
    success: bool, fail_log: Optional[str] = None,
    row_count: Optional[int] = None, data_gb: Optional[float] = None
):
    status = 'success' if success else 'fail'
    duration_sec = round(end_ts - start_ts, 2)
    sql = """
        UPDATE mg_flow.mstr_cube_list_log
           SET end_ts = to_timestamp(%s),
               status = %s,
               fail_log = %s,
               duration_sec = %s,
               row_count = %s,
               data_gb = %s
         WHERE flow_date   = CURRENT_DATE
           AND flow_run_no = %s
           AND guid        = %s
           AND start_ts    = to_timestamp(%s)
           AND status      = 'running'
    """
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(sql, (end_ts, status, fail_log, duration_sec, row_count, data_gb, run_no, guid, start_ts))
        if cur.rowcount == 0:
            # safety: if running row is not found, insert a terminal row
            sql_ins = """
                INSERT INTO mg_flow.mstr_cube_list_log
                (flow_date, flow_run_no, guid, start_ts, end_ts, status, fail_log, duration_sec, row_count, data_gb)
                VALUES (CURRENT_DATE, %s, %s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, %s)
            """
            cur.execute(sql_ins, (run_no, guid, start_ts, end_ts, status, fail_log, duration_sec, row_count, data_gb))

# -------------------- email (optional) --------------------

def send_email(subject: str, body: str):
    if not (SMTP_HOST and MAIL_FROM and MAIL_TO):
        return  # email disabled
    try:
        import smtplib
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = MAIL_FROM
        msg["To"] = ", ".join(MAIL_TO)
        msg.set_content(body)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=60) as s:
            s.send_message(msg)
    except Exception:
        # best-effort only
        pass

# -------------------- mstr helpers --------------------

def mstr_connect() -> Connection:
    if not (MSTR_BASE_URL and MSTR_USERNAME and MSTR_PASSWORD and MSTR_PROJECT):
        raise RuntimeError("Missing MicroStrategy env: MSTR_BASE_URL / MSTR_USERNAME / MSTR_PASSWORD / MSTR_PROJECT")
    conn = Connection(
        base_url=MSTR_BASE_URL,
        username=MSTR_USERNAME,
        password=MSTR_PASSWORD,
        login_mode=MSTR_LOGIN_MODE,
        project_name=MSTR_PROJECT,
    )
    conn.connect()
    return conn

def _refresh_and_wait(connection: Connection, cube_guid: str) -> Tuple[bool, str]:
    cube = OlapCube(connection=connection, id=cube_guid)
    job = cube.refresh()
    job_id = getattr(job, "id", None)

    status = None
    if job_id:
        j = Job(connection=connection, id=job_id)
        deadline = time.time() + WAIT_TIMEOUT_SEC
        while time.time() < deadline:
            j.refresh_status()
            status = getattr(j, "status", None)
            if status in (JobStatus.COMPLETED, JobStatus.ERROR, JobStatus.STOPPED, JobStatus.CANCELING):
                break
            time.sleep(POLL_INTERVAL_SEC)

    if status == JobStatus.COMPLETED:
        return True, "COMPLETED"
    return False, f"job_status={status or 'NO_JOB_OR_TIMEOUT'}"

def _get_cache_metrics_retry(connection: Connection, cube_guid: str, attempts: int = 3, pause_sec: int = 5) -> Tuple[Optional[int], Optional[float]]:
    """
    list_cube_caches -> rowCount, size (KB). We convert KB to GB for storage.
    After refresh, cache visibility may lag; retry a few times.
    """
    last = (None, None)
    for _ in range(attempts):
        try:
            caches = list_cube_caches(connection, cube_id=cube_guid, to_dictionary=True, limit=50)
            if caches:
                try:
                    caches_sorted = sorted(caches, key=lambda c: c.get("lastUpdateTime") or "", reverse=True)
                    c0 = caches_sorted[0]
                except Exception:
                    c0 = caches[0]
                row_count = c0.get("rowCount")
                size_kb = c0.get("size")
                data_gb = float(size_kb) / 1024.0 / 1024.0 if size_kb is not None else None
                return row_count, data_gb
        except Exception:
            pass
        time.sleep(pause_sec)
    return last

# -------------------- single cube run --------------------

def format_hms(seconds: float) -> str:
    s = int(round(seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"

def run_one(guid: str, name: str, run_no: int, connection: Connection) -> dict:
    start_ts = time.time()
    try:
        log_running(guid, run_no, start_ts)
    except Exception:
        pass

    try:
        ok, note = _refresh_and_wait(connection, guid)
        end_ts = time.time()
        rc, gb = _get_cache_metrics_retry(connection, guid, attempts=3, pause_sec=5)
        try:
            log_update_result(
                guid, run_no, start_ts, end_ts, success=ok,
                fail_log=None if ok else note, row_count=rc, data_gb=gb
            )
        except Exception:
            pass
        return {
            "guid": guid,
            "name": name,
            "status": "success" if ok else "fail",
            "duration_sec": round(end_ts - start_ts, 2),
            "row_count": rc,
            "data_gb": gb
        }
    except Exception as e:
        end_ts = time.time()
        try:
            log_update_result(guid, run_no, start_ts, end_ts, success=False, fail_log=repr(e),
                              row_count=None, data_gb=None)
        except Exception:
            pass
        return {
            "guid": guid,
            "name": name,
            "status": "fail",
            "duration_sec": round(end_ts - start_ts, 2),
            "row_count": None,
            "data_gb": None,
            "error": repr(e)
        }

# -------------------- main --------------------

def main():
    parser = argparse.ArgumentParser(description="Refresh MicroStrategy OLAP cubes and log results.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all", action="store_true", help="Run all active cubes.")
    group.add_argument("--missing-today", action="store_true",
                       help="Run only cubes that do not have a 'success' record for today.")
    parser.add_argument("--max-workers", type=int, default=MAX_WORKERS, help="Parallel workers.")
    parser.add_argument("--dry-run", action="store_true", help="List target cubes and exit.")
    args = parser.parse_args()

    host = socket.gethostname()

    if args.missing_today:
        items = fetch_cubes_without_success_today()
        mode = "missing_today"
    else:
        # default to --all if neither given
        items = fetch_active_cubes_all()
        mode = "all"

    if not items:
        send_email("[MSTR Cubes] No Work",
                   f"{host}: No cubes to run for mode={mode}.")
        print("No cubes to run.")
        return

    if args.dry_run:
        print(json.dumps({"mode": mode, "count": len(items),
                          "cubes": [{"guid": g, "order": o, "name": n} for g, o, n in items]},
                         ensure_ascii=True, indent=2))
        return

    run_no = compute_today_flow_run_no()

    # start email
    start_lines = [f"{i+1:02d}) {name}" for i, (_, _, name) in enumerate(items)]
    send_email(f"[MSTR Cubes] Started (run #{run_no}, mode={mode})",
               f"{host}: {len(items)} cubes scheduled.\n"
               f"Parallel: {args.max_workers}\n"
               f"Run No (today): {run_no}\n\n" + "\n".join(start_lines))

    conn = mstr_connect()

    # Note: mstrio's Connection object is thread-safe for issuing independent
    # REST calls, so we intentionally share a single connection across the
    # worker threads instead of creating one per cube run.

    results = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        fut = {pool.submit(run_one, guid, name, run_no, conn): (guid, name) for guid, _, name in items}
        for f in as_completed(fut):
            try:
                results.append(f.result())
            except Exception as e:
                guid, name = fut[f]
                results.append({"guid": guid, "name": name, "status": "fail",
                                "duration_sec": 0.0, "row_count": None, "data_gb": None, "error": repr(e)})

    # summary
    ok = sum(1 for r in results if r["status"] == "success")
    fail = len(results) - ok

    # preserve original order
    name_to_idx = {name: i for i, (_, _, name) in enumerate(items)}
    results_sorted = sorted(results, key=lambda r: name_to_idx.get(r["name"], 999999))

    # finish email
    lines_done = []
    for i, r in enumerate(results_sorted, 1):
        dur = r.get("duration_sec") or 0.0
        dur_hms = format_hms(dur)
        parts = [f"{dur_hms} ({dur:.2f}s)"]
        if r.get("row_count") is not None:
            parts.append(f"rows={r['row_count']}")
        if r.get("data_gb") is not None:
            parts.append(f"size_gb={r['data_gb']:.3f}")
        suffix = f" — {r['status']}"
        if parts:
            suffix += " [" + ", ".join(parts) + "]"
        lines_done.append(f"{i:02d}) {r['name']}{suffix}")

    summary = {"run_no": run_no, "mode": mode, "total": len(results), "success": ok, "fail": fail}

    send_email(f"[MSTR Cubes] Finished (run #{run_no}, mode={mode})",
               f"{host}: summary={json.dumps(summary, ensure_ascii=True)}\n\n" + "\n".join(lines_done))

    print(json.dumps({"summary": summary, "details": results_sorted}, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()