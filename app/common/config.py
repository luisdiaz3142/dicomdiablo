"""
config.py
=========
mercure's configuration management, used by various mercure modules.
Supports standalone mercure.json file or shared PostgreSQL config (see config_backend).
"""

# Standard python includes
import json
import os
from pathlib import Path
from typing import Dict, cast

import common.helper as helper
# App-specific includes
import common.monitor as monitor
import common.tagslist as tagslist
from common.config_backend import get_config_backend
from common.constants import mercure_names
from common.log_helpers import get_logger
from common.types import Config
from typing_extensions import Literal

# Create local logger instance
logger = get_logger()

configuration_timestamp: float = 0
_os_config_file = os.getenv("MERCURE_CONFIG_FILE")
if _os_config_file is not None:
    configuration_filename = _os_config_file
else:
    configuration_filename = (os.getenv("MERCURE_CONFIG_FOLDER") or "/opt/mercure/config") + "/mercure.json"

_config_backend = None
_notify_listener_started = False


def _get_backend():
    global _config_backend
    if _config_backend is None:
        _config_backend = get_config_backend(config_file_path=configuration_filename)
    return _config_backend


def _invalidate_config_timestamp() -> None:
    """Called when shared config DB sends NOTIFY so next read_config() refetches."""
    global configuration_timestamp
    configuration_timestamp = 0

_os_mercure_basepath = os.getenv("MERCURE_BASEPATH")
if _os_mercure_basepath is None:
    app_basepath = Path(__file__).resolve().parent.parent
else:
    app_basepath = Path(_os_mercure_basepath)

mercure_defaults = {
    "appliance_name": "master",
    "appliance_color": "#FFF",
    "port": 11112,
    "accept_compressed_images": False,
    "incoming_folder": "/opt/mercure/data/incoming",
    "studies_folder": "/opt/mercure/data/studies",
    "outgoing_folder": "/opt/mercure/data/outgoing",
    "success_folder": "/opt/mercure/data/success",
    "error_folder": "/opt/mercure/data/error",
    "discard_folder": "/opt/mercure/data/discard",
    "processing_folder": "/opt/mercure/data/processing",
    "jobs_folder": "/opt/mercure/data/jobs",
    "persistence_folder": "/opt/mercure/persistence",
    "router_scan_interval": 1,  # in seconds
    "dispatcher_scan_interval": 1,  # in seconds
    "cleaner_scan_interval": 60,  # in seconds
    "retention": 259200,  # in seconds (3 days)
    "emergency_clean_percentage": 90,  # in % of disk space
    "retry_delay": 900,  # in seconds (15 min)
    "retry_max": 5,
    "series_complete_trigger": 60,  # in seconds
    "study_complete_trigger": 900,  # in seconds
    "study_forcecomplete_trigger": 5400,  # in seconds
    "dicom_receiver": {"additional_tags": []},
    "graphite_ip": "",
    "graphite_port": 2003,
    "influxdb_host": "",
    "influxdb_org": "",
    "influxdb_token": "",
    "influxdb_bucket": "",
    "bookkeeper": "0.0.0.0:8080",
    "offpeak_start": "22:00",
    "offpeak_end": "06:00",
    "process_runner": "docker",
    "targets": {},
    "rules": {},
    "modules": {},
    "features": {"dummy_target": False},
    "processing_logs": {"discard_logs": False},
    "email_notification_from": "mercure@mercure.mercure",
    "support_root_modules": False,
    "phi_notifications": False,
    "server_time": "UTC",
    "local_time": "UTC",
}

mercure: Config


def read_config() -> Config:
    """Reads the configuration (from file or shared DB). Only reloads when the stored version is newer than the last load.
    If using file backend and the file is locked by another process, raises ResourceWarning."""
    global mercure
    global configuration_timestamp
    global _notify_listener_started
    backend = _get_backend()
    configuration_file = Path(configuration_filename)

    # File backend only: check lock file before read
    if hasattr(backend, "config_file_path") and backend.config_file_path == configuration_file:
        lock_file = Path(configuration_file.parent / configuration_file.stem).with_suffix(mercure_names.LOCK)
        if lock_file.exists():
            raise ResourceWarning(f"Configuration file locked: {lock_file}")

    try:
        loaded_config, version = backend.read()
    except FileNotFoundError as e:
        raise FileNotFoundError(str(e))

    # Skip reload if we already have this version (or newer)
    if version <= configuration_timestamp and configuration_timestamp > 0:
        return mercure

    logger.info("Reading configuration from: %s", configuration_filename)

    merged: Dict = {**mercure_defaults, **loaded_config}
    mercure = Config(**merged)

    if not check_folders():
        raise FileNotFoundError("Configured folders missing")

    try:
        read_tagslist()
    except Exception as e:
        logger.info(e)
        logger.info("Unable to parse list of additional tags. Check configuration file.")

    configuration_timestamp = version
    monitor.send_event(monitor.m_events.CONFIG_UPDATE, monitor.severity.INFO, "Configuration updated")

    # Start NOTIFY listener once when using shared DB (so we reload when another server updates config)
    if backend.supports_notify() and not _notify_listener_started:
        _notify_listener_started = True
        backend.start_notify_listener(on_notify=_invalidate_config_timestamp)

    return mercure


def save_config() -> None:
    """Saves the current configuration (to file or shared DB). With file backend, raises if the file is locked."""
    global configuration_timestamp, mercure
    backend = _get_backend()
    configuration_file = Path(configuration_filename)

    # File backend only: use lock file
    if hasattr(backend, "config_file_path") and backend.config_file_path == configuration_file:
        lock_file = Path(configuration_file.parent / configuration_file.stem).with_suffix(mercure_names.LOCK)
        if lock_file.exists():
            raise ResourceWarning(f"Configuration file locked: {lock_file}")
        try:
            lock = helper.FileLock(lock_file)
        except Exception:
            raise ResourceWarning(f"Unable to lock configuration file: {lock_file}")
    else:
        lock = None

    backend.write(mercure.dict())

    if hasattr(backend, "config_file_path") and backend.config_file_path == configuration_file:
        try:
            configuration_timestamp = configuration_file.stat().st_mtime
        except OSError:
            configuration_timestamp = 0
    else:
        # DB backend: refresh timestamp from backend (next read would do it; set optimistically)
        try:
            _, configuration_timestamp = backend.read()
        except Exception:
            pass

    monitor.send_event(monitor.m_events.CONFIG_UPDATE, monitor.severity.INFO, "Saved new configuration.")
    logger.info("Stored configuration into: %s", configuration_filename)

    if lock is not None:
        try:
            lock.free()
        except Exception:
            logger.error("Unable to remove lock file %s", lock_file, None)
            return


def write_configfile(json_content) -> None:
    """Rewrites the config using the JSON data passed as argument (file or shared DB). Used by the config editor."""
    global configuration_timestamp
    backend = _get_backend()
    configuration_file = Path(configuration_filename)

    if hasattr(backend, "config_file_path") and backend.config_file_path == configuration_file:
        lock_file = Path(configuration_file.parent / configuration_file.stem).with_suffix(mercure_names.LOCK)
        if lock_file.exists():
            raise ResourceWarning(f"Configuration file locked: {lock_file}")
        try:
            lock = helper.FileLock(lock_file)
        except Exception:
            raise ResourceWarning(f"Unable to lock configuration file: {lock_file}")
    else:
        lock = None

    backend.write(json_content)

    if hasattr(backend, "config_file_path") and backend.config_file_path == configuration_file:
        try:
            configuration_timestamp = configuration_file.stat().st_mtime
        except OSError:
            configuration_timestamp = 0
    else:
        try:
            _, configuration_timestamp = backend.read()
        except Exception:
            pass

    monitor.send_event(monitor.m_events.CONFIG_UPDATE, monitor.severity.INFO, "Wrote configuration file.")
    logger.info("Wrote configuration into: %s", configuration_filename)

    if lock is not None:
        try:
            lock.free()
        except Exception:
            logger.error("Unable to remove lock file %s", lock_file, None)
            return


def check_folders() -> bool:
    """Checks if all required folders for handling the DICOM files exist."""
    global mercure

    for entry in [
        "incoming_folder",
        "studies_folder",
        "outgoing_folder",
        "success_folder",
        "error_folder",
        "discard_folder",
        "processing_folder",
    ]:
        entry = cast(
            Literal[
                "incoming_folder",
                "studies_folder",
                "outgoing_folder",
                "success_folder",
                "error_folder",
                "discard_folder",
                "processing_folder",
            ],
            entry,
        )
        if not Path(mercure.dict()[entry]).exists():

            logger.critical(  # handle_error
                f"Folder not found {mercure.dict()[entry]}",
                None,
                event_type=monitor.m_events.CONFIG_UPDATE,
            )
            return False
    return True


def read_tagslist() -> None:
    """Reads the list of supported DICOM tags with example values, displayed the UI."""
    global mercure
    tagslist.alltags = {**tagslist.default_tags, **mercure.dicom_receiver.additional_tags}
    tagslist.sortedtags = sorted(tagslist.alltags)
