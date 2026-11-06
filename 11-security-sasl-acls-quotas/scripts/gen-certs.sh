#!/usr/bin/env bash
# Generate a lab CA + broker keystore/truststore + client cert for SSL / mTLS experiments (KiA 10.1).
# Requires: openssl and keytool (any JDK). Output: 11-security-sasl-acls-quotas/certs/
#
#   bash 11-security-sasl-acls-quotas/scripts/gen-certs.sh
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=certs; PW=changeit; DAYS=365
mkdir -p $OUT && cd $OUT

# 1. Our own Certificate Authority (in real life: your company CA / Vault / cert-manager)
openssl req -new -x509 -nodes -days $DAYS -subj "/CN=kafka-lab-ca" -keyout ca.key -out ca.crt

# 2. Broker key pair + CSR. SAN must contain every hostname clients use (ssl.endpoint.identification.algorithm=https)
openssl req -new -nodes -newkey rsa:2048 -subj "/CN=kafka-secure" -keyout broker.key -out broker.csr
printf "subjectAltName=DNS:kafka-secure,DNS:localhost" > san.ext
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days $DAYS -extfile san.ext -out broker.crt

# 3. Client cert (mTLS: the CN becomes the principal  ->  User:CN=alice)
openssl req -new -nodes -newkey rsa:2048 -subj "/CN=alice" -keyout alice.key -out alice.csr
openssl x509 -req -in alice.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days $DAYS -out alice.crt

# 4. Java keystore/truststore for the broker (PKCS12)
openssl pkcs12 -export -in broker.crt -inkey broker.key -certfile ca.crt -name kafka-secure \
  -out broker.keystore.p12 -password pass:$PW
if keytool -help >/dev/null 2>&1; then   # macOS ships a keytool stub even without a JDK
  keytool -importcert -noprompt -alias ca -file ca.crt -keystore broker.truststore.p12 -storetype PKCS12 -storepass $PW
else
  echo "keytool not found: create the truststore with any JDK, or use ssl.truststore.type=PEM with ca.crt"
fi
echo "$PW" > credentials
ls -l
cat <<TXT

Broker settings (add an SSL listener, e.g. SSL://0.0.0.0:9193):
  ssl.keystore.location=/etc/kafka/certs/broker.keystore.p12   ssl.keystore.type=PKCS12  ssl.keystore.password=$PW
  ssl.truststore.location=/etc/kafka/certs/broker.truststore.p12 ssl.truststore.type=PKCS12 ssl.truststore.password=$PW
  ssl.client.auth=required          # => mTLS
Python client:
  {"security.protocol": "SSL", "ssl.ca.location": "certs/ca.crt",
   "ssl.certificate.location": "certs/alice.crt", "ssl.key.location": "certs/alice.key"}
TXT
