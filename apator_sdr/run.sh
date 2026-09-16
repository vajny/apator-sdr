#!/bin/sh
set -e
mkdir -p /data /config
if [ ! -f /config/devices.json ] && [ -f /opt/apator/devices.json ]; then
  cp /opt/apator/devices.json /config/devices.json
fi
export WEB_PORT="${WEB_PORT:-8099}"
exec python3 /opt/apator/apator.py
