# Gunicorn configuration for production deployment
import multiprocessing
import os

# Server socket
bind = "0.0.0.0:5000"
backlog = 2048

# Worker processes
# - gthread: each worker handles connections in a thread pool, so a slow TLS
#   handshake only blocks one thread, not the whole accept loop. This is what
#   fixed the "ERR_TIMED_OUT after some days" symptom on HTTPS:5000.
# - 2 workers gives HA while one is being recycled by max_requests.
workers = 2
worker_class = "gthread"
threads = 8
worker_connections = 1000
timeout = 30               # kills a worker stuck >30s (incl. bad TLS handshakes)
graceful_timeout = 30
keepalive = 2

# Restart workers after this many requests, to prevent FD/memory leaks from
# accumulating over long runs.
max_requests = 1000
max_requests_jitter = 50

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Process naming
proc_name = "stock_service"

# SSL Configuration
keyfile = "key.pem"
certfile = "cert.pem"

# Security
limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190

# Performance
preload_app = False  # Disable preload to avoid macOS fork issues
daemon = False
pidfile = None
user = None
group = None
tmp_upload_dir = None

# SSL Context
ssl_version = "TLSv1_2"
ciphers = "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS"
