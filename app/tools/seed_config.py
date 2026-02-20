#!/usr/bin/env python3
"""
seed_config.py
==============
Seeds the diablo_config table with default configuration from default_mercure.json.
Run during installation when using shared (database) configuration mode, or when
joining an existing cluster (no-op if row already exists).

Usage:
  Set DATABASE_URL and run from the app directory:
    cd /opt/mercure/app && python3 -m tools.seed_config

  Or with env file:
    source /opt/mercure/config/mercure.env && cd /opt/mercure/app && python3 -m tools.seed_config
"""
import json
import os
import sys

# Ensure app is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main() -> int:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is not set.", file=sys.stderr)
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
