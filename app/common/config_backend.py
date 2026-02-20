"""
config_backend.py
=================
Backend abstraction for configuration storage.
Supports two modes:
  - "file": JSON file on disk (original behavior, standalone deployments)
  - "database": PostgreSQL-backed (shared configuration for multi-server deployments)

The backend is selected via the MERCURE_CONFIG_BACKEND environment variable.
"""

import json
import os
import socket
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional

from common.constants import mercure_names
from common.log_helpers import get_logger

logger = get_logger()


class ConfigBackend(ABC):
    """Abstract interface for configuration storage."""

    @abstractmethod
    def load(self) -> Optional[Dict]:
        """Load configuration. Returns config dict if changed since last load, else None."""
        ...

    @abstractmethod
    def save(self, config_dict: Dict, updated_by: str = "") -> None:
        """Persist configuration dict."""
        ...

    @abstractmethod
    def load_raw(self) -> str:
        """Return raw JSON string for the configuration editor."""
        ...

    @abstractmethod
    def save_raw(self, json_content: Dict) -> None:
        """Write raw JSON content (from the configuration editor)."""
        ...

    @abstractmethod
    def is_locked(self) -> bool:
        """Check if configuration is currently locked by another writer."""
        ...


class FileBackend(ConfigBackend):
    """JSON file-based configuration storage (original behavior).

    Uses file modification timestamp for change detection and .lock files for concurrency.
    """

    def __init__(self, config_filename: str):
        self._config_filename = config_filename
        self._timestamp: float = 0

    @property
    def config_path(self) -> Path:
        return Path(self._config_filename)

    @property
    def lock_path(self) -> Path:
        cfg = self.config_path
        return Path(cfg.parent / cfg.stem).with_suffix(mercure_names.LOCK)

    def is_locked(self) -> bool:
        return self.lock_path.exists()

    def load(self) -> Optional[Dict]:
        if self.is_locked():
            raise ResourceWarning(f"Configuration file locked: {self.lock_path}")

        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        stat = os.stat(self._config_filename)
        try:
            timestamp = stat.st_mtime
        except AttributeError:
            timestamp = 0

        if timestamp <= self._timestamp:
            return None

        logger.info(f"Reading configuration from: {self._config_filename}")
        with open(self.config_path, "r") as json_file:
            loaded_config = json.load(json_file)

        self._timestamp = timestamp
        return loaded_config

    def save(self, config_dict: Dict, updated_by: str = "") -> None:
        if self.is_locked():
            raise ResourceWarning(f"Configuration file locked: {self.lock_path}")

        from common.helper import FileLock
        try:
            lock = FileLock(self.lock_path)
        except Exception:
            raise ResourceWarning(f"Unable to lock configuration file: {self.lock_path}")

        try:
            with open(self.config_path, "w") as json_file:
                json.dump(config_dict, json_file, indent=4)

            try:
                stat = os.stat(self.config_path)
                self._timestamp = stat.st_mtime
            except AttributeError:
                self._timestamp = 0
        finally:
            try:
                lock.free()
            except Exception:
                logger.error(f"Unable to remove lock file {self.lock_path}", None)

    def load_raw(self) -> str:
        if self.is_locked():
            raise ResourceWarning("Configuration is being updated. Try again in a minute.")

        with open(self.config_path, "r") as json_file:
            config_content = json.load(json_file)
        return json.dumps(config_content, indent=4, sort_keys=False)

    def save_raw(self, json_content: Dict) -> None:
        if self.is_locked():
            raise ResourceWarning(f"Configuration file locked: {self.lock_path}")

        from common.helper import FileLock
        try:
            lock = FileLock(self.lock_path)
        except Exception:
            raise ResourceWarning(f"Unable to lock configuration file: {self.lock_path}")

        try:
            with open(self.config_path, "w") as json_file:
                json.dump(json_content, json_file, indent=4)
        finally:
            try:
                lock.free()
            except Exception:
                logger.error(f"Unable to remove lock file {self.lock_path}", None)


class DatabaseBackend(ConfigBackend):
    """PostgreSQL-backed configuration storage for multi-server deployments.

    Uses a singleton row in the diablo_config table with a version counter for
    change detection and SELECT FOR UPDATE for concurrency control.

    Maintains a local JSON file cache for:
      - The receiver shell script (reads config with jq)
      - Fallback if the database is temporarily unreachable
    """

    def __init__(self, database_url: str, cache_path: str):
        self._database_url = database_url
        self._cache_path = Path(cache_path)
        self._last_version: int = 0
        self._conn = None
        self._hostname = socket.gethostname()

    def _get_connection(self):
        """Get or create a synchronous psycopg2 connection."""
        import psycopg2
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._database_url)
            self._conn.autocommit = False
        return self._conn

    def _ensure_table(self) -> None:
        """Ensure the diablo_config table exists. Called on first connection."""
        conn = self._get_connection()
        try:
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
        except Exception:
            conn.rollback()
            raise

    def _write_cache(self, config_dict: Dict) -> None:
        """Write config to local cache file for receiver.sh and fallback."""
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._cache_path, "w") as f:
                json.dump(config_dict, f, indent=4)
        except Exception as e:
            logger.warning(f"Failed to write config cache to {self._cache_path}: {e}")

    def _read_cache(self) -> Optional[Dict]:
        """Read config from local cache file as fallback."""
        try:
            if self._cache_path.exists():
                with open(self._cache_path, "r") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read config cache from {self._cache_path}: {e}")
        return None

    def is_locked(self) -> bool:
        # Database backend uses row-level locking, no persistent lock state
        return False

    def load(self) -> Optional[Dict]:
        import psycopg2
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                # Quick version check
                cur.execute("SELECT version FROM diablo_config WHERE id = 1")
                row = cur.fetchone()
                if row is None:
                    raise FileNotFoundError(
                        "No configuration found in database. "
                        "Run seed_config.py to initialize."
                    )

                current_version = row[0]
                if current_version <= self._last_version:
                    conn.commit()
                    return None

                # Version changed, fetch full config
                cur.execute("SELECT config_data, version FROM diablo_config WHERE id = 1")
                row = cur.fetchone()
                config_data = row[0]
                self._last_version = row[1]
            conn.commit()

            logger.info(f"Read configuration from database (version {self._last_version})")

            # Ensure config_data is a dict (psycopg2 auto-deserializes JSONB)
            if isinstance(config_data, str):
                config_data = json.loads(config_data)

            # Update local cache
            self._write_cache(config_data)

            return config_data

        except psycopg2.Error as e:
            logger.warning(f"Database error reading config: {e}")
            # Try to reset connection for next attempt
            try:
                if self._conn and not self._conn.closed:
                    self._conn.close()
            except Exception:
                pass
            self._conn = None

            # If we have no config loaded yet, try cache
            if self._last_version == 0:
                cached = self._read_cache()
                if cached is not None:
                    logger.warning("Using cached configuration as fallback")
                    return cached
                raise FileNotFoundError(
                    "Database unreachable and no cached configuration available."
                )
            # Otherwise return None (keep using in-memory config)
            return None

    def save(self, config_dict: Dict, updated_by: str = "") -> None:
        if not updated_by:
            updated_by = self._hostname

        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # Row-level lock
                cur.execute("SELECT id FROM diablo_config WHERE id = 1 FOR UPDATE")
                row = cur.fetchone()
                if row is None:
                    raise FileNotFoundError("No configuration row found in database.")

                cur.execute(
                    """UPDATE diablo_config
                       SET config_data = %s,
                           version = version + 1,
                           updated_at = NOW(),
                           updated_by = %s
                       WHERE id = 1
                       RETURNING version""",
                    (json.dumps(config_dict), updated_by)
                )
                new_version = cur.fetchone()[0]
                self._last_version = new_version

            conn.commit()
            logger.info(f"Saved configuration to database (version {self._last_version})")

            # Update local cache
            self._write_cache(config_dict)

        except Exception:
            conn.rollback()
            raise

    def load_raw(self) -> str:
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT config_data FROM diablo_config WHERE id = 1")
                row = cur.fetchone()
                if row is None:
                    raise FileNotFoundError("No configuration found in database.")
                config_data = row[0]
            conn.commit()

            if isinstance(config_data, str):
                config_data = json.loads(config_data)
            return json.dumps(config_data, indent=4, sort_keys=False)

        except Exception:
            conn.rollback()
            raise

    def save_raw(self, json_content: Dict) -> None:
        self.save(json_content, updated_by=f"{self._hostname} (editor)")

    def get_version_info(self) -> Optional[Dict]:
        """Get metadata about the current config version. Used by the web UI."""
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT version, updated_at, updated_by FROM diablo_config WHERE id = 1"
                )
                row = cur.fetchone()
            conn.commit()
            if row:
                return {
                    "version": row[0],
                    "updated_at": row[1].isoformat() if row[1] else None,
                    "updated_by": row[2],
                }
        except Exception as e:
            logger.warning(f"Failed to get config version info: {e}")
            try:
                if self._conn and not self._conn.closed:
                    self._conn.rollback()
            except Exception:
                pass
        return None


def create_backend() -> ConfigBackend:
    """Factory function to create the appropriate backend based on environment."""
    backend_type = os.getenv("MERCURE_CONFIG_BACKEND", "file").lower()

    if backend_type == "database":
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError(
                "MERCURE_CONFIG_BACKEND=database requires DATABASE_URL environment variable"
            )
        cache_path = os.getenv("MERCURE_CONFIG_CACHE",
                               (os.getenv("MERCURE_CONFIG_FOLDER") or "/opt/mercure/config")
                               + "/mercure.json.cache")
        backend = DatabaseBackend(database_url, cache_path)
        backend._ensure_table()
        logger.info(f"Using database configuration backend (cache: {cache_path})")
        return backend

    elif backend_type == "file":
        _os_config_file = os.getenv("MERCURE_CONFIG_FILE")
        if _os_config_file is not None:
            config_filename = _os_config_file
        else:
            config_filename = (os.getenv("MERCURE_CONFIG_FOLDER") or "/opt/mercure/config") + "/mercure.json"
        logger.info(f"Using file configuration backend ({config_filename})")
        return FileBackend(config_filename)

    else:
        raise ValueError(f"Unknown MERCURE_CONFIG_BACKEND value: {backend_type}")
