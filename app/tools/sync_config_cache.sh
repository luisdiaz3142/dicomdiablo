#!/bin/bash
# Syncs config from the database backend to the local cache file.
# Use as ExecStartPre in the receiver systemd unit when MERCURE_CONFIG_BACKEND=database,
# so the receiver (which reads config via jq from a file) gets an up-to-date cache.
# When MERCURE_CONFIG_BACKEND=file, the receiver reads mercure.json directly; this script is optional.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"
if [ -x "/opt/mercure/env/bin/python" ]; then
  PYTHON="/opt/mercure/env/bin/python"
else
  PYTHON="python3"
fi
cd "$APP_DIR"
$PYTHON -c "
from common.config import read_config
read_config()
"
