#!/usr/bin/env python3
"""
seed_config.py
==============
Seeds the diablo_config table with default configuration from default_mercure.json.
Run during installation when using shared (database) configuration mode, or when
joining an existing cluster (no-op if row already exists).

Usage:
  From the app directory (loads DATABASE_URL from mercure.env if not set):
    cd /opt/mercure/app && python3 -m tools.seed_config

  Or set DATABASE_URL and run:
    export DATABASE_URL=postgresql://user:password@host/dbname
    cd /opt/mercure/app && python3 -m tools.seed_config

  Or source the env file first:
    source /opt/mercure/config/mercure.env && cd /opt/mercure/app && python3 -m tools.seed_config
"""
import json
import os
import sys

# Ensure app is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _load_mercure_env() -> None:
    """Load DATABASE_URL from mercure.env if not already set (same path as systemd services)."""
    if os.getenv("DATABASE_URL"):
        return
    config_folder = os.getenv("MERCURE_CONFIG_FOLDER") or "/opt/mercure/config"
    env_path = os.path.join(config_folder, "mercure.env")
    if not os.path.isfile(env_path):
        return
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                # Strip quotes and inline comments
                value = value.split("#")[0].strip().strip("'\"")
                if key == "DATABASE_URL" and value:
                    os.environ["DATABASE_URL"] = value
                    return


def main() -> int:
    _load_mercure_env()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is not set.", file=sys.stderr)
        print("  Set it or add DATABASE_URL=... to your mercure.env (e.g. /opt/mercure/config/mercure.env).", file=sys.stderr)
        return 1

    default_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "configuration",
        "default_mercure.json",
    )
    if not os.path.isfile(default_path):
        print(f"ERROR: Default config not found: {default_path}", file=sys.stderr)
        return 1

    with open(default_path, "r") as f:
        default_config = json.load(f)

    import psycopg2

    conn = None
    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = False
        with conn.cursor() as cur:
            # Create table if not exists (matches DatabaseBackend._ensure_table)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS diablo_config (
                    id          INTEGER PRIMARY KEY DEFAULT 1,
                    config_data JSONB NOT NULL,
                    version     INTEGER NOT NULL DEFAULT 1,
                    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_by  VARCHAR(255) DEFAULT '',
                    CONSTRAINT  singleton CHECK (id = 1)
                );
            """)
            conn.commit()

            cur.execute("SELECT id FROM diablo_config WHERE id = 1")
            row = cur.fetchone()
            if row is not None:
                print("Joining existing shared configuration (row id=1 already present).")
                return 0

            cur.execute(
                """INSERT INTO diablo_config (id, config_data, version, updated_by)
                   VALUES (1, %s, 1, 'seed_config.py')
                """,
                (json.dumps(default_config),),
            )
        conn.commit()
        print("Seeded default configuration into diablo_config.")
        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        return 1
    finally:
        if conn and not conn.closed:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
