#!/usr/bin/env bash
# Non-interactive TLS material for the integration clusters.
# PKCS12 keystore/truststore (no keytool) plus a PEM CA for Node clients.
# `*-java8.p12` is OpenSSL 3 `-legacy` (3DES) for Confluent 5.x / Java 8.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CERTS="${ROOT}/test/assets/certs"
PASSWORD="testtest"
VALIDITY_DAYS=3650
CN="localhost"

rm -rf "${CERTS}"
mkdir -p "${CERTS}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

openssl req -new -x509 -nodes -days "${VALIDITY_DAYS}" \
  -keyout "${WORKDIR}/ca.key" \
  -out "${WORKDIR}/ca.crt" \
  -subj "/CN=kafka-test-ca"

openssl req -new -nodes \
  -keyout "${WORKDIR}/kafka.key" \
  -out "${WORKDIR}/kafka.csr" \
  -subj "/CN=${CN}"

cat > "${WORKDIR}/san.cnf" <<EOF
subjectAltName=DNS:localhost,DNS:kafka1,DNS:kafka2,DNS:kafka3,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
EOF

openssl x509 -req -days "${VALIDITY_DAYS}" \
  -in "${WORKDIR}/kafka.csr" \
  -CA "${WORKDIR}/ca.crt" \
  -CAkey "${WORKDIR}/ca.key" \
  -CAcreateserial \
  -out "${WORKDIR}/kafka.crt" \
  -extfile "${WORKDIR}/san.cnf"

openssl pkcs12 -export \
  -in "${WORKDIR}/kafka.crt" \
  -inkey "${WORKDIR}/kafka.key" \
  -certfile "${WORKDIR}/ca.crt" \
  -name localhost \
  -out "${CERTS}/kafka.server.keystore.p12" \
  -passout "pass:${PASSWORD}"

openssl pkcs12 -export \
  -nokeys \
  -in "${WORKDIR}/ca.crt" \
  -name CARoot \
  -out "${CERTS}/kafka.server.truststore.p12" \
  -passout "pass:${PASSWORD}"

# Confluent 5.x / Java 8 cannot parse OpenSSL 3's default PBES2 PKCS12
# (`parseAlgParameters failed`). Emit a 3DES/legacy copy for ZooKeeper stacks.
openssl pkcs12 -export -legacy \
  -in "${WORKDIR}/kafka.crt" \
  -inkey "${WORKDIR}/kafka.key" \
  -certfile "${WORKDIR}/ca.crt" \
  -name localhost \
  -out "${CERTS}/kafka.server.keystore-java8.p12" \
  -passout "pass:${PASSWORD}"

openssl pkcs12 -export -legacy \
  -nokeys \
  -in "${WORKDIR}/ca.crt" \
  -name CARoot \
  -out "${CERTS}/kafka.server.truststore-java8.p12" \
  -passout "pass:${PASSWORD}"

cp "${WORKDIR}/ca.crt" "${CERTS}/cert-signed"
cp "${WORKDIR}/ca.crt" "${CERTS}/ca.crt"

printf '%s\n' "${PASSWORD}" > "${CERTS}/keystore_creds"
printf '%s\n' "${PASSWORD}" > "${CERTS}/sslkey_creds"
printf '%s\n' "${PASSWORD}" > "${CERTS}/truststore_creds"
chmod 644 "${CERTS}"/*

echo "Wrote TLS material to ${CERTS}"
