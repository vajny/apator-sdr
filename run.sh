#!/bin/sh
set -e
mkdir -p /data
if [ ! -f /data/devices.json ] && [ -f /opt/apator/devices.json ]; then
  cp /opt/apator/devices.json /data/devices.json
fi
export WEB_PORT="${WEB_PORT:-8099}"
exec python3 /opt/apator/apator.py
