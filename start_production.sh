#!/bin/bash

# Production startup script.
#
# server.py is the single entry point. It launches:
#   * HTTPS on :5000  — gunicorn subprocess (wsgi:application + gunicorn.conf.py)
#   * HTTP  on :5001  — Werkzeug in-process thread
#
# Ctrl+C stops both. See server.py for the supervisor logic.

set -euo pipefail

cd "$(dirname "$0")"

echo "Starting Stock Service in Production Mode..."
echo "=============================================="

# Pick an interpreter: prefer the project's .venv if present.
if [ -x ".venv/bin/python" ]; then
    PY=".venv/bin/python"
    PIP=".venv/bin/pip"
elif [ -x "venv/bin/python" ]; then
    PY="venv/bin/python"
    PIP="venv/bin/pip"
else
    echo "Creating virtual environment at .venv..."
    python3 -m venv .venv
    PY=".venv/bin/python"
    PIP=".venv/bin/pip"
fi

# Install/update dependencies (idempotent).
echo "Ensuring dependencies are installed..."
"$PIP" install -q -r requirements.txt

# server.py generates cert.pem/key.pem on demand (via wsgi.ensure_self_signed_cert)
# and raises the FD limit itself, so we just exec it here.
exec "$PY" server.py
