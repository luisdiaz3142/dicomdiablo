#!/usr/bin/env python3
"""
Bootstrap shared config DB: create mercure_config table and insert config from
mercure.json. Used by install.sh when shared config database is selected.
Reads MERCURE_CONFIG_DATABASE_URL and MERCURE_CONFIG_FOLDER from environment.
"""
import json
import os
import sys
from pathlib import Path

# Run from app directory so common is importable
app_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(app_dir))
os.chdir(app_dir)

def main():
    db_url = os.environ.get("MERCURE_CONFIG_DATABASE_URL")
    config_folder = os.environ.get("MERCURE_CONFIG_FOLDER", "/opt/mercure/config")
    if not db_url:
        print("MERCURE_CONFIG_DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    config_file = Path(config_folder) / "mercure.json"
    from common.config_backend import DatabaseConfigBackend
    backend = DatabaseConfigBackend(database_url=db_url, cache_file_path=str(config_file))
    conn = backend._get_connection()
    try:
        backend._ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM " + DatabaseConfigBackend.TABLE_NAME + " WHERE id = 1")
            has_row = cur.fetchone() is not None
    finally:
        conn.close()
    if not has_row:
        if not config_file.exists():
            print("Config file not found:", config_file, file=sys.stderr)
            print("Copy default_mercure.json to mercure.json or run install on the primary server first.", file=sys.stderr)
            sys.exit(1)
        with open(config_file) as f:
            data = json.load(f)
        backend.write(data)
        print("Shared config table created and initial config inserted.")
    else:
        # DB already has config (e.g. another server); just refresh local cache for receiver
        backend.read()
        print("Shared config already present; local cache file updated.")

if __name__ == "__main__":
    main()
