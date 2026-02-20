====================================
Shared Configuration (Multi-Server)
====================================

The application supports two configuration storage modes:

- **File (standalone)** — Configuration is stored in ``mercure.json`` (default). Suitable for a single server.
- **Database (shared)** — Configuration is stored in PostgreSQL. All application servers share the same config; changes made on one node are visible to others after the next poll.

Choosing the backend
--------------------

Set the environment variable ``MERCURE_CONFIG_BACKEND``:

- ``file`` (default) — Use the JSON file at ``MERCURE_CONFIG_FOLDER``/mercure.json (or ``MERCURE_CONFIG_FILE`` if set).
- ``database`` — Use the PostgreSQL table ``diablo_config``. Requires ``DATABASE_URL``.

Example (e.g. in ``/opt/mercure/config/mercure.env``)::

  MERCURE_CONFIG_BACKEND=file

  # For shared mode:
  # MERCURE_CONFIG_BACKEND=database
  # DATABASE_URL=postgresql://user:password@host:5432/mercure

All services (web UI, router, dispatcher, receiver, etc.) should have the same backend and, for database mode, the same ``DATABASE_URL``. No code changes are required at call sites; ``read_config()``, ``save_config()``, and ``config.mercure`` behave the same for both backends.

Database schema
---------------

When using ``database`` backend, the config is stored in a single row::

  CREATE TABLE diablo_config (
      id          INTEGER PRIMARY KEY DEFAULT 1,
      config_data JSONB NOT NULL,
      version     INTEGER NOT NULL DEFAULT 1,
      updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      updated_by  VARCHAR(255) DEFAULT ''
  );
  -- Constraint: only row with id=1 (singleton).

Apply the schema with Alembic::

  cd /opt/mercure/app && alembic upgrade head

Or let the application create the table on first use (``DatabaseBackend._ensure_table()``).

Receiver and cache file
-----------------------

The DICOM receiver is a shell script that reads config with ``jq`` from a file. In **file** mode it reads ``mercure.json``. In **database** mode it must read a local cache file that the Python backend updates whenever it loads config from the DB.

1. Set the cache path (optional; default is ``MERCURE_CONFIG_FOLDER``/mercure.json.cache)::

     MERCURE_CONFIG_CACHE=/opt/mercure/config/mercure.json.cache

2. Point the receiver at the cache::

     MERCURE_CONFIG_FILE=/opt/mercure/config/mercure.json.cache

3. Before starting the receiver, refresh the cache so it has the latest config from the DB. For example, in the receiver’s systemd unit::

     ExecStartPre=/opt/mercure/app/tools/sync_config_cache.sh

   Then start the receiver as usual. ``sync_config_cache.sh`` calls ``read_config()``, which loads from the DB and writes the cache file.

Seeding and migration
---------------------

- **New install (shared mode)**  
  After creating the DB and table, seed the default config::

    export DATABASE_URL=postgresql://...
    cd /opt/mercure/app && python3 -m tools.seed_config

  If row ``id=1`` already exists, ``seed_config`` does nothing (join existing cluster).

- **Migrate from file to database**  
  On the server that currently has the active ``mercure.json``::

    cd /opt/mercure/app && python3 -m tools.migrate_config_to_db \
      --config-file /opt/mercure/config/mercure.json \
      --database-url postgresql://user:pass@host/db

  Then set ``MERCURE_CONFIG_BACKEND=database`` and ``DATABASE_URL``, configure the receiver to use the cache (see above), and restart all services.

Verification
-----------

- **File mode**: Edit ``mercure.json`` or use the web UI; other services see changes after their next ``read_config()`` (e.g. on poll).
- **Database mode**: Edit config from any node’s web UI; all nodes see the same config. Check that the receiver starts and uses the cache file when ``MERCURE_CONFIG_FILE`` points to the cache.
