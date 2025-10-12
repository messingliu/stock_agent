#!/bin/bash

# macOS-compatible startup script for Stock Service
# This script avoids macOS fork() issues by using Flask's development server

echo "Starting Stock Service on macOS..."
echo "=================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install/update dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Generate SSL certificates if they don't exist
if [ ! -f "cert.pem" ] || [ ! -f "key.pem" ]; then
    echo "Generating SSL certificates..."
    python3 -c "
import ssl
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import datetime

# Generate private key
private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

# Generate certificate
subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u'localhost')])
cert = x509.CertificateBuilder().subject_name(subject).issuer_name(issuer).public_key(private_key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(datetime.datetime.utcnow()).not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365)).add_extension(x509.SubjectAlternativeName([x509.DNSName(u'localhost')]), critical=False).sign(private_key, hashes.SHA256())

# Save certificate and private key
with open('cert.pem', 'wb') as f:
    f.write(cert.public_bytes(serialization.Encoding.PEM))

with open('key.pem', 'wb') as f:
    f.write(private_key.private_bytes(encoding=serialization.Encoding.PEM, format=serialization.PrivateFormat.PKCS8, encryption_algorithm=serialization.NoEncryption()))

print('SSL certificates generated successfully!')
"
fi

# Start the service with Flask (macOS-compatible)
echo "Starting Flask server (macOS-compatible)..."
echo "Server will be available at: https://0.0.0.0:5000"
echo "Press Ctrl+C to stop the server"
echo ""

# Use Flask's development server with optimized settings for macOS
python3 stock_service.py
