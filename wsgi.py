#!/usr/bin/env python3
"""
WSGI entry point for production deployment.

Gunicorn loads the certificate via the `certfile`/`keyfile` options in
`gunicorn.conf.py`, so this module only needs to:
  1) make sure cert.pem/key.pem exist (generate a self-signed pair if not), and
  2) expose `application = app` for gunicorn.
"""

import os
import datetime

from stock_service import app


def ensure_self_signed_cert(cert_path: str = "cert.pem", key_path: str = "key.pem") -> None:
    """Generate a self-signed cert+key if either file is missing.

    Gunicorn will load these at startup via its certfile/keyfile settings.
    """
    if os.path.exists(cert_path) and os.path.exists(key_path):
        return

    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    print("[wsgi] Generating self-signed SSL certificate...")
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),
    ])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
            critical=False,
        )
        .sign(private_key, hashes.SHA256())
    )

    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(key_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ))
    print(f"[wsgi] SSL certificate written to {cert_path} / {key_path}")


ensure_self_signed_cert()

# Export the Flask app for Gunicorn (`gunicorn wsgi:application`).
application = app


if __name__ == "__main__":
    # Direct execution is only for smoke-testing without gunicorn.
    # In production, use: gunicorn --config gunicorn.conf.py wsgi:application
    app.run(host="0.0.0.0", port=5000, debug=False)
