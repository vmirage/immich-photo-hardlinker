import os
import time
import json
import sqlite3
import psycopg2
from datetime import datetime, timezone
from pathlib import Path

# ================= ENV HELPERS =================

def env(name, default):
    return os.getenv(name, default)

# ================= HELPERS =================

def parse_iso(dt_str):
    if isinstance(dt_str, datetime):
        return dt_str
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

# ================= CONFIG (ENV OVERRIDABLE) =================

PG_CONFIG = {
    "host": env("PG_HOST", "database"),
    "port": int(env("PG_PORT", "5432")),
    "dbname": env("PG_DATABASE_NAME", "immich"),
    "user": env("PG_USERNAME", "postgres"),
    "password": env("PG_PASSWORD", "password"),
}

FIRST_RUN_MIN_CREATE_DATE = parse_iso(env("FIRST_RUN_MIN_CREATE_DATE", "1970-01-01T00:00:00Z"))
SYNC_INTERVAL = int(env("SYNC_INTERVAL", "30"))
DB_PATH = env("SQLITE_DB", "./sync.db")

# PATH MAP (JSON from env)
PATH_MAP = json.loads(
    env(
        "PATH_MAP",
        '{"/data/library": "/data/library.link"}'
    )
)

# ================= DB =================

def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS assets (
        asset_id TEXT PRIMARY KEY,
        file_path TEXT,
        exported INTEGER DEFAULT 0,
        updated_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS state (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    conn.commit()
    return conn


def get_state(conn, key):
    cur = conn.cursor()
    cur.execute("SELECT value FROM state WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None


def set_state(conn, key, value):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO state (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, str(value)))
    conn.commit()


# ================= IMMICH =================

def pg_conn():
    return psycopg2.connect(**PG_CONFIG)

def fetch_assets(pg, last_sync):
    cur = pg.cursor()

    if last_sync is None:
        last_sync = "1970-01-01T00:00:00Z"

    cur.execute("""
        SELECT id, "originalPath", "updatedAt", "fileCreatedAt"
        FROM asset
        WHERE "deletedAt" IS NULL
          AND "updatedAt" > %s
          AND "originalPath" NOT LIKE '/data/upload/%%'
        ORDER BY "updatedAt" ASC
    """, (last_sync,))

    return cur.fetchall()


# ================= SQL HELPERS =================

def get_asset_status(cur, asset_id):
    cur.execute(
        "SELECT exported FROM assets WHERE asset_id=?",
        (asset_id,)
    )
    return cur.fetchone()


def upsert_asset(cur, asset_id, file_path, updated_at):
    cur.execute("""
        INSERT INTO assets (asset_id, file_path, exported, updated_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT(asset_id) DO UPDATE SET
            file_path=excluded.file_path,
            exported=1,
            updated_at=excluded.updated_at
    """, (
        asset_id,
        file_path,
        updated_at.isoformat() if hasattr(updated_at, "isoformat") else str(updated_at)
    ))


# ================= PATH RESOLUTION =================

def resolve_destination(src_path):
    for src_root, dst_root in sorted(PATH_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        if src_path.startswith(src_root):
            rel = os.path.relpath(src_path, src_root)
            return Path(dst_root) / rel

    return None


def hardlink(src_path):
    dst = resolve_destination(src_path)

    if dst is None:
        print(f"[SKIP] unmapped path: {src_path}")        
        return

    os.makedirs(dst.parent, exist_ok=True)

    if not dst.exists():
        os.link(src_path, dst)

    print(f"[LINK] {src_path} -> {dst}")


# ================= CORE =================

def process_batch(conn, assets, first_run, fist_run_min_create_date):
    cur = conn.cursor()

    max_updated = None

    for asset_id, file_path, updated_at, file_created_at in assets:

        if not file_path or not os.path.exists(file_path):
            continue

        if first_run and file_created_at < fist_run_min_create_date:
            upsert_asset(cur, asset_id, file_path, updated_at)
            conn.commit()
            continue

        row = get_asset_status(cur, asset_id)

        if row and row[0] == 1:
            continue

        try:
            upsert_asset(cur, asset_id, file_path, updated_at)
            hardlink(file_path)
            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"[FAIL] {asset_id} {file_path}: {e}")

        if not max_updated or updated_at > max_updated:
            max_updated = updated_at

    return max_updated


# ================= MAIN LOOP =================
def main():
    print("Starting...")
    conn = init_db()
    print("Started")

    firstrun_complete = get_state(conn, "firstrun_complete") == "1"
    last_sync = get_state(conn, "last_sync")

    if last_sync is None:
        last_sync = "1970-01-01T00:00:00Z"

    try:
        while True:
            pg = None
            try:
                pg = pg_conn()
                assets = fetch_assets(pg, last_sync)

            finally:
                if pg is not None:
                    pg.close()

            try:
                if assets:
                    new_sync = process_batch(
                        conn,
                        assets,
                        not firstrun_complete,
                        FIRST_RUN_MIN_CREATE_DATE
                    )

                    if new_sync:
                        set_state(conn, "last_sync", str(new_sync))

                    if not firstrun_complete:
                        set_state(conn, "firstrun_complete", "1")
                        firstrun_complete = True
                        print("[FIRST RUN] completed")

                time.sleep(SYNC_INTERVAL)

            except Exception as e:
                print("[ERROR]", e)
                time.sleep(5)

    finally:
        conn.close()


if __name__ == "__main__":
    main()