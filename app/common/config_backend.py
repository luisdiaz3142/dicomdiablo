"""
config_backend.py
=================
Backend abstraction for mercure configuration: file-based (standalone) or
PostgreSQL (shared across servers). When using the database backend, config
is synced via a single source of truth; updates from any server are visible
to all and can trigger reload via polling or LISTEN/NOTIFY.
"""

import json
import os
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from common.log_helpers import get_logger

logger = get_logger()

# Channel used for Postgres NOTIFY when config is updated (all servers listening will reload)
MERCURE_CONFIG_NOTIFY_CHANNEL = "mercure_config_update"


def get_config_backend(
    config_folder: Optional[str] = None,
    config_file_path: Optional[str] = None,
    config_db_url_env: str = "MERCURE_CONFIG_DATABASE_URL",
) -> "ConfigBackend":
    """
    Returns the appropriate config backend based on environment.
    If MERCURE_CONFIG_DATABASE_URL is set, use database backend; otherwise file.
    """
    config_folder = config_folder or os.getenv("MERCURE_CONFIG_FOLDER", "/opt/mercure/config")
    config_file_path = config_file_path or (config_folder.rstrip("/") + "/mercure.json")
    db_url = os.getenv(config_db_url_env)
    if db_url:
        return DatabaseConfigBackend(
            database_url=db_url,
            cache_file_path=config_file_path,
        )
    return FileConfigBackend(config_file_path=config_file_path)


class ConfigBackend(ABC):
    """Abstract base for configuration storage."""

    @abstractmethod
    def read(self) -> Tuple[Dict[str, Any], float]:
        """
        Read config as JSON-serializable dict and a version/timestamp.
        Returns (config_dict, version). Version is used to detect changes.
        """
        pass

    @abstractmethod
    def write(self, config_dict: Dict[str, Any]) -> None:
        """Write full config. For DB backend this notifies other servers."""
        pass

    def supports_notify(self) -> bool:
        """Whether this backend can signal remote reload (e.g. LISTEN/NOTIFY)."""
        return False

    def start_notify_listener(self, on_notify: Optional[Callable[[], None]] = None) -> None:
        """Start background listener for config change notifications (no-op if not supported)."""
        pass

    def stop_notify_listener(self) -> None:
        """Stop background listener if any."""
        pass


class FileConfigBackend(ConfigBackend):
    """Standalone config: single mercure.json file. Reload is driven by file mtime."""

    def __init__(self, config_file_path: str) -> None:
        self.config_file_path = Path(config_file_path)

    def read(self) -> Tuple[Dict[str, Any], float]:
        if not self.config_file_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_file_path}")
        with open(self.config_file_path, "r") as f:
            data = json.load(f)
        try:
            version = self.config_file_path.stat().st_mtime
        except OSError:
            version = 0.0
        return data, version

    def write(self, config_dict: Dict[str, Any]) -> None:
        self.config_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_file_path, "w") as f:
            json.dump(config_dict, f, indent=4)
        return


class DatabaseConfigBackend(ConfigBackend):
    """
    Shared config in PostgreSQL. One row holds the current config JSON.
    Updates from any server are written to the DB and NOTIFY is sent so
    other servers can reload without waiting for their next poll.
    """

    TABLE_NAME = "mercure_config"
    LOCK_SUFFIX = ".lock"

    def __init__(self, database_url: str, cache_file_path: str) -> None:
        self.database_url = database_url
        self.cache_file_path = Path(cache_file_path)
        self._listener_conn = None
        self._listener_thread: Optional[threading.Thread] = None
        self._listener_stop = threading.Event()
        self._on_notify_callback: Optional[Callable[[], None]] = None

    def _get_connection(self):
        import psycopg2
        return psycopg2.connect(self.database_url)

    def _ensure_table(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mercure_config (
                    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                    config_json JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        conn.commit()

    def read(self) -> Tuple[Dict[str, Any], float]:
        conn = self._get_connection()
        try:
            self._ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT config_json, EXTRACT(EPOCH FROM updated_at) FROM {self.TABLE_NAME} WHERE id = 1"
                )
                row = cur.fetchone()
            if not row:
                raise FileNotFoundError(
                    f"No config row in database. Run install with shared config or insert default config into {self.TABLE_NAME}."
                )
            data, updated_epoch = row[0], float(row[1])
            # JSONB returns dict; ensure we have a plain dict
            if hasattr(data, "copy"):
                data = data.copy()
            else:
                data = json.loads(json.dumps(data))
            self._write_cache(data)
            return data, updated_epoch
        finally:
            conn.close()

    def _write_cache(self, config_dict: Dict[str, Any]) -> None:
        """Keep local mercure.json in sync so receiver.sh and jq still work."""
        try:
            self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file_path, "w") as f:
                json.dump(config_dict, f, indent=4)
        except Exception as e:
            logger.warning("Could not write config cache file for receiver: %s", e)

    def write(self, config_dict: Dict[str, Any]) -> None:
        conn = self._get_connection()
        try:
            conn.set_isolation_level(0)  # autocommit for NOTIFY
            self._ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.TABLE_NAME} (id, config_json, updated_at)
                    VALUES (1, %s::jsonb, NOW())
                    ON CONFLICT (id) DO UPDATE SET config_json = EXCLUDED.config_json, updated_at = NOW()
                    """,
                    (json.dumps(config_dict),),
                )
                cur.execute("NOTIFY " + MERCURE_CONFIG_NOTIFY_CHANNEL)
            self._write_cache(config_dict)
        finally:
            conn.close()

    def supports_notify(self) -> bool:
        return True

    def start_notify_listener(self, on_notify: Optional[Callable[[], None]] = None) -> None:
        """Start a thread that LISTENs for NOTIFY and calls on_notify() when config changes."""
        if self._listener_thread is not None and self._listener_thread.is_alive():
            return
        self._on_notify_callback = on_notify
        self._listener_stop.clear()
        self._listener_thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._listener_thread.start()
        logger.info("Config NOTIFY listener started (channel=%s)", MERCURE_CONFIG_NOTIFY_CHANNEL)

    def _listen_loop(self) -> None:
        import psycopg2
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        conn = None
        while not self._listener_stop.is_set():
            try:
                conn = psycopg2.connect(self.database_url)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cur:
                    cur.execute("LISTEN " + MERCURE_CONFIG_NOTIFY_CHANNEL)
                while not self._listener_stop.is_set():
                    if conn.poll() != psycopg2.extensions.POLL_OK:
                        break
                    conn.select()
                    while conn.notifies:
                        n = conn.notifies.pop(0)
                        logger.info("Config NOTIFY received: %s", n.channel)
                        if self._on_notify_callback:
                            try:
                                self._on_notify_callback()
                            except Exception as e:
                                logger.exception("Error in config notify callback: %s", e)
            except Exception as e:
                if not self._listener_stop.is_set():
                    logger.warning("Config LISTEN connection error, will retry: %s", e)
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
            self._listener_stop.wait(timeout=5)

    def stop_notify_listener(self) -> None:
        self._listener_stop.set()
        if self._listener_thread:
            self._listener_thread.join(timeout=10)
        self._listener_thread = None
