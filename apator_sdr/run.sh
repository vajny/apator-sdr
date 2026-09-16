#!/bin/sh
set -e
mkdir -p /data /config
export WEB_PORT="${WEB_PORT:-8099}"
exec python3 /opt/apator/apator.py
