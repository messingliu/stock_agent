"""Single entry point for the stock service.

`python server.py` starts:
  * HTTPS on :5000 — served by gunicorn (subprocess, using `wsgi:application`
    and `gunicorn.conf.py`). Gunicorn uses the `gthread` worker so a slow/
    broken TLS handshake blocks one worker thread instead of wedging the
    whole accept loop. This is the fix for the "ERR_TIMED_OUT after some
    days" symptom.
  * HTTP  on :5001 — served by Werkzeug in a thread inside this process.

Ctrl+C (SIGINT) / SIGTERM tears both down. If either side dies on its own,
the supervisor loop initiates a full shutdown so we never end up half-alive.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback

from werkzeug.serving import make_server

from stock_service import app
from config import config


# ---------- helpers ----------------------------------------------------------

def check_port_available(host: str, port: int) -> bool:
    """Return True if nothing is currently listening on (host, port)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            return s.connect_ex((host, port)) != 0
    except Exception as e:
        print(f"[MAIN] Error checking port {port}: {e}")
        return False


def ensure_gunicorn_available() -> None:
    try:
        import gunicorn  # noqa: F401
    except ImportError:
        print("[MAIN] ERROR: gunicorn is not installed in this environment.")
        print("[MAIN] Install with: pip install -r requirements.txt")
        sys.exit(1)


def ensure_ssl_cert() -> None:
    """Make sure cert.pem/key.pem exist before gunicorn tries to load them.

    `wsgi.ensure_self_signed_cert()` is a no-op if both files already exist.
    """
    import wsgi  # importing triggers ensure_self_signed_cert()
    _ = wsgi


# ---------- HTTP (Werkzeug, in-process) --------------------------------------

class HttpServerThread(threading.Thread):
    def __init__(self, host: str, port: int) -> None:
        super().__init__(name="HTTP-Server", daemon=True)
        self._httpd = make_server(host, port, app, threaded=True)
        self.host = host
        self.port = port

    def run(self) -> None:
        print(f"[HTTP]  Serving on http://{self.host}:{self.port}")
        try:
            self._httpd.serve_forever()
        except Exception as e:
            print(f"[HTTP]  error: {e}")
            traceback.print_exc()

    def shutdown(self) -> None:
        try:
            self._httpd.shutdown()
        except Exception as e:
            print(f"[HTTP]  shutdown error: {e}")


# ---------- HTTPS (gunicorn subprocess) --------------------------------------

def start_https_subprocess() -> subprocess.Popen:
    """Launch gunicorn as a child process.

    `start_new_session=True` puts gunicorn in its own process group, so a
    Ctrl+C on the TTY reaches *this* process only and we forward it
    explicitly. That avoids gunicorn receiving two SIGINTs (one from the
    terminal, one from us) and skipping graceful shutdown.
    """
    cmd = [
        sys.executable, "-m", "gunicorn",
        "--config", "gunicorn.conf.py",
        "wsgi:application",
    ]
    print(f"[HTTPS] Launching gunicorn: {' '.join(cmd)}")
    return subprocess.Popen(cmd, start_new_session=True)


def stop_https_subprocess(proc: subprocess.Popen, grace_seconds: int = 15) -> None:
    if proc.poll() is not None:
        return
    pgid = os.getpgid(proc.pid)
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        print(f"[HTTPS] gunicorn didn't stop in {grace_seconds}s, sending SIGKILL")
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


# ---------- supervisor -------------------------------------------------------

def main() -> None:
    web_config = config.get("web_service")
    host = web_config["host"]
    http_port = web_config["http_port"]
    https_port = web_config["https_port"]
    ssl_enabled = web_config["ssl"]["enabled"]

    # Pre-flight port checks.
    if not check_port_available(host, http_port):
        print(f"[MAIN] ERROR: HTTP port {http_port} is already in use.")
        sys.exit(1)
    if ssl_enabled and not check_port_available(host, https_port):
        print(f"[MAIN] ERROR: HTTPS port {https_port} is already in use.")
        sys.exit(1)

    # Belt-and-suspenders: raise the per-process FD limit so a burst of bad
    # TLS handshakes can't exhaust the default 256 on macOS.
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < 4096:
            resource.setrlimit(resource.RLIMIT_NOFILE, (min(4096, hard), hard))
    except Exception:
        pass

    # Start HTTP (in-process thread).
    http_thread = HttpServerThread(host, http_port)
    http_thread.start()

    # Start HTTPS (gunicorn subprocess).
    gunicorn_proc: subprocess.Popen | None = None
    if ssl_enabled:
        ensure_gunicorn_available()
        ensure_ssl_cert()
        gunicorn_proc = start_https_subprocess()
    else:
        print("[HTTPS] disabled in config; only HTTP is running")

    # Signal handling — idempotent so re-entry from the supervisor is safe.
    shutting_down = threading.Event()

    def shutdown(signum, _frame=None) -> None:
        if shutting_down.is_set():
            return
        shutting_down.set()
        print(f"\n[MAIN] Received signal {signum}, shutting down...")
        http_thread.shutdown()
        if gunicorn_proc is not None:
            stop_https_subprocess(gunicorn_proc)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Give both sides a moment to come up, then print status.
    time.sleep(2)
    http_ok = http_thread.is_alive()
    https_ok = gunicorn_proc is None or gunicorn_proc.poll() is None
    if not http_ok:
        print("[MAIN] ERROR: HTTP thread died immediately!")
    if not https_ok:
        print(f"[MAIN] ERROR: gunicorn exited immediately with code {gunicorn_proc.returncode}!")
    if http_ok and https_ok:
        print("[MAIN] All servers started successfully. Press Ctrl+C to stop.")
    else:
        shutdown(signal.SIGTERM)
        sys.exit(1)

    # Supervise: if either side dies, tear the other down and exit.
    try:
        while not shutting_down.is_set():
            if not http_thread.is_alive():
                print("[MAIN] HTTP thread died — shutting down")
                break
            if gunicorn_proc is not None:
                rc = gunicorn_proc.poll()
                if rc is not None:
                    print(f"[MAIN] gunicorn exited with code {rc} — shutting down")
                    break
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    shutdown(signal.SIGTERM)
    print("[MAIN] done")


if __name__ == "__main__":
    main()
