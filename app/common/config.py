"""
config.py
=========
mercure's configuration management, used by various mercure modules.

Supports two storage backends:
  - "file" (default): JSON file on disk (standalone deployments)
  - "database": PostgreSQL-backed (multi-server shared configuration)

The backend is selected via the MERCURE_CONFIG_BACKEND environment variable.
All existing call sites (read_config, save_config, write_configfile, config.mercure)
work identically regardless of backend.
"""

# Standard python includes
import os
from pathlib import Path
from typing import Dict, Optional, cast

# App-specific includes
import common.monitor as monitor
import common.tagslist as tagslist
from common.config_backend import ConfigBackend, create_backend
from common.log_helpers import get_logger
from common.types import Config
from typing_extensions import Literal

# Create local logger instance
logger = get_logger()

# Kept for backward compatibility — points to the JSON file path.
# In database mode, MERCURE_CONFIG_FILE can point to the cache file for the receiver.
configuration_timestamp: float = 0
_os_config_file = os.getenv("MERCURE_CONFIG_FILE")
if _os_config_file is not None:
    configuration_filename = _os_config_file
else:
    configuration_filename = (os.getenv("MERCURE_CONFIG_FOLDER") or "/opt/mercure/config") + "/mercure.json"

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

# Backend instance — lazily initialized on first use
_backend: Optional[ConfigBackend] = None


def get_backend() -> ConfigBackend:
    """Get the configuration backend, creating it on first call."""
    global _backend
    if _backend is None:
        _backend = create_backend()
    return _backend


def read_config() -> Config:
    """Reads the configuration settings (rules, targets, general settings) from the
    configuration backend. The configuration will only be updated if it has changed
    since the last function call. If the configuration is locked by another process,
    an exception will be raised."""
    global mercure

    backend = get_backend()
    loaded_config = backend.load()

    if loaded_config is None:
        # No changes since last read
        return mercure

    # Merge with defaults to ensure all needed keys are present
    merged: Dict = {**mercure_defaults, **loaded_config}
    mercure = Config(**merged)

    # Check if directories exist
    if not check_folders():
        raise FileNotFoundError("Configured folders missing")

    try:
        read_tagslist()
    except Exception as e:
        logger.info(e)
        logger.info("Unable to parse list of additional tags. Check configuration file.")

    monitor.send_event(monitor.m_events.CONFIG_UPDATE, monitor.severity.INFO, "Configuration updated")
    return mercure


def save_config() -> None:
    """Saves the current configuration to the backend. Raises an exception if the
    configuration is locked by another process."""
    global mercure

    backend = get_backend()
    backend.save(mercure.dict())

    monitor.send_event(monitor.m_events.CONFIG_UPDATE, monitor.severity.INFO, "Saved new configuration.")
    logger.info("Stored configuration.")


def write_configfile(json_content) -> None:
    """Rewrites the config using the JSON data passed as argument. Used by the config editor of the webgui."""
    backend = get_backend()
    backend.save_raw(json_content)

    monitor.send_event(monitor.m_events.CONFIG_UPDATE, monitor.severity.INFO, "Wrote configuration file.")
    logger.info("Wrote configuration via editor.")


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
