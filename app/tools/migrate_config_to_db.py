#!/usr/bin/env python3
"""
migrate_config_to_db.py
=======================
Migrates an existing mercure.json file into the shared diablo_config table.
Run on a standalone server before switching to MERCURE_CONFIG_BACKEND=database.

Usage:
  python3 -m tools.migrate_config_to_db --config-file /opt/mercure/config/mercure.json --database-url postgresql://user:pass@host/db

After running:
  1. Set MERCURE_CONFIG_BACKEND=database and DATABASE_URL in mercure.env
  2. Optionally set MERCURE_CONFIG_FILE to the cache path for the receiver, e.g. /opt/mercure/config/mercure.json.cache
  3. Restart all mercure services
"""
import argparse
import json
import os
import sys

# Ensure app is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate mercure.json to shared database config")
    parser.add_argument("--config-file", required=True, help="Path to existing mercure.json")
    parser.add_argument("--database-url", required=True, help="PostgreSQL connection URL")
    args = parser.parse_args()

    if not os.path.isfile(args.config_file):
        print(f"ERROR: Config file not found: {args.config_file}", file=sys.stderr)
        return 1

    with open(args.config_file, "r") as f:
        config_data = json.load(f)

    import psycopg2

    conn = None
    try:
        conn = psycopg2.connect(args.database_url)
        conn.autocommit = False
        with conn.cursor() as cur:
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
            if cur.fetchone() is not None:
                print("ERROR: diablo_config already has a row (id=1). Refusing to overwrite.", file=sys.stderr)
                print("If you intend to replace it, delete the row manually first.", file=sys.stderr)
                return 1

            cur.execute(
                """INSERT INTO diablo_config (id, config_data, version, updated_by)
                   VALUES (1, %s, 1, 'migrate_config_to_db.py')
                """,
                (json.dumps(config_data),),
            )
        conn.commit()
        print("Migration complete. Configuration has been written to the database.")
        print("")
        print("Next steps:")
        print("  1. In /opt/mercure/config/mercure.env set:")
        print("     MERCURE_CONFIG_BACKEND=database")
        print(f"     DATABASE_URL={args.database_url}")
        print("  2. Restart all mercure services.")
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
