#!/usr/bin/env python3
"""
WSGI entry point for production deployment
"""

import os
import ssl
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import datetime

# Import the Flask app
from stock_service import app

def create_ssl_context():
    """Create SSL context for production"""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    
    # Check if certificate files already exist
    cert_path = "cert.pem"
    key_path = "key.pem"
    
    if not os.path.exists(cert_path) or not os.path.exists(key_path):
        print("Generating SSL certificate...")
        
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        
        # Generate certificate
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u"localhost")
        ])
        
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            datetime.datetime.utcnow() + datetime.timedelta(days=365)
        ).add_extension(
            x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
            critical=False,
        ).sign(private_key, hashes.SHA256())
        
        # Save certificate and private key
        with open(cert_path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        
        with open(key_path, "wb") as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            ))
        
        print(f"SSL certificate saved to {cert_path}")
        print(f"SSL private key saved to {key_path}")
    
    return context

# Create SSL context
ssl_context = create_ssl_context()

# Export the application for Gunicorn
application = app

if __name__ == "__main__":
    # This allows running the WSGI app directly for testing
    app.run(
        host='0.0.0.0',
        port=5000,
        ssl_context=ssl_context,
        debug=False
    )
