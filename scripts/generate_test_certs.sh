#!/bin/bash

# Script to generate self-signed TLS certificates for testing Kawasan
# Usage: ./scripts/generate_test_certs.sh [output_dir]

set -e

OUTPUT_DIR="${1:-/tmp/kawasan-test-certs}"
VALIDITY_DAYS=365

echo "Generating test TLS certificates in: $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

cd "$OUTPUT_DIR"

# Generate CA private key and certificate
echo "Generating CA certificate..."
openssl genrsa -out ca-key.pem 2048
openssl req -new -x509 -days $VALIDITY_DAYS -key ca-key.pem -out ca-cert.pem \
    -subj "/C=US/ST=Test/L=Test/O=Kawasan Test CA/CN=kawasan-test-ca"

# Generate server private key and certificate signing request
echo "Generating server certificate..."
openssl genrsa -out server-key.pem 2048
openssl req -new -key server-key.pem -out server-req.pem \
    -subj "/C=US/ST=Test/L=Test/O=Kawasan/CN=localhost"

# Sign server certificate with CA
openssl x509 -req -days $VALIDITY_DAYS -in server-req.pem \
    -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
    -out server-cert.pem

# Generate client private key and certificate signing request
echo "Generating client certificate..."
openssl genrsa -out client-key.pem 2048
openssl req -new -key client-key.pem -out client-req.pem \
    -subj "/C=US/ST=Test/L=Test/O=Kawasan Client/CN=client"

# Sign client certificate with CA
openssl x509 -req -days $VALIDITY_DAYS -in client-req.pem \
    -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
    -out client-cert.pem

# Clean up temporary files
rm -f server-req.pem client-req.pem ca-cert.srl

echo ""
echo "Certificate generation complete!"
echo ""
echo "Files created in $OUTPUT_DIR:"
echo "  ca-cert.pem       - CA certificate (for client trust store)"
echo "  ca-key.pem        - CA private key"
echo "  server-cert.pem   - Server certificate"
echo "  server-key.pem    - Server private key"
echo "  client-cert.pem   - Client certificate"
echo "  client-key.pem    - Client private key"
echo ""
echo "To use with Kawasan, add to config file:"
echo "  ssl.enabled=true"
echo "  ssl.cert.file=$OUTPUT_DIR/server-cert.pem"
echo "  ssl.key.file=$OUTPUT_DIR/server-key.pem"
echo "  ssl.ca.file=$OUTPUT_DIR/ca-cert.pem"
echo ""
