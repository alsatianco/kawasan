#!/usr/bin/env bash
# Isolated SASL/PLAIN acceptance/rejection against the selected implementation.
set -euo pipefail
TARGET="${1:-kawasan}"
DIR="$(mktemp -d /tmp/kawasan-sasl-XXXXXX)"
NAME="kawasan-harness-sasl-$$"
cleanup() {
    result=$?
    docker logs "$NAME" > "${ECOSYSTEM_EVIDENCE_DIR:-/tmp}/$NAME.log" 2>&1 || true
    if [[ "$result" != 0 ]]; then tail -30 "${ECOSYSTEM_EVIDENCE_DIR:-/tmp}/$NAME.log"; fi
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    rm -rf "$DIR"
}
trap cleanup EXIT
chmod 755 "$DIR"
if [[ "$TARGET" == kafka ]]; then
    echo 'KafkaServer { org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="wonderland-42" user_alice="wonderland-42"; };' > "$DIR/jaas.conf"
    chmod 644 "$DIR/jaas.conf"
    docker run -d --name "$NAME" -p 9097:9097 -v "$DIR:/sasl:ro" \
        -e KAFKA_OPTS=-Djava.security.auth.login.config=/sasl/jaas.conf \
        -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_LISTENERS=SASL_PLAINTEXT://0.0.0.0:9097,CONTROLLER://0.0.0.0:9093 \
        -e KAFKA_ADVERTISED_LISTENERS=SASL_PLAINTEXT://localhost:9097 \
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=SASL_PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN \
        -e KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN \
        -e KAFKA_INTER_BROKER_LISTENER_NAME=SASL_PLAINTEXT \
        -e 'KAFKA_LISTENER_NAME_SASL_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="wonderland-42" user_alice="wonderland-42";' \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        apache/kafka:4.2.0
else
    printf 'alice:wonderland-42\n' > "$DIR/credentials"
    cat > "$DIR/broker.properties" <<'CONFIG'
broker.id=0
host=0.0.0.0
advertised.host=localhost
port=9097
raft.port=9093
log.dirs=/tmp/kawasan-sasl-data
monitoring.enabled=false
sasl.plain.credentials.file=/sasl/credentials
CONFIG
    chmod 644 "$DIR"/*
    IMAGE=$(docker inspect -f '{{.Image}}' "${HARNESS_BROKER_CONTAINER:-kawasan-harness-candidate}")
    docker run -d --name "$NAME" -p 9097:9097 -v "$DIR:/sasl:ro" \
        "$IMAGE" kawasan-broker --config /sasl/broker.properties
fi
python3 - <<'PY'
import time
from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError

def producer(user, password):
    return Producer({'bootstrap.servers':'localhost:9097', 'security.protocol':'SASL_PLAINTEXT',
                     'sasl.mechanism':'PLAIN', 'sasl.username':user, 'sasl.password':password,
                     'message.timeout.ms':10000, 'socket.timeout.ms':5000, 'log_level':0})
p = producer('alice', 'wonderland-42')
deadline = time.monotonic() + 90
while True:
    try:
        p.list_topics(timeout=3); break
    except Exception:
        if time.monotonic() >= deadline: raise
        time.sleep(1)
errors = []
for i in range(5):
    p.produce('compat-sasl', value=f'sasl-{i}', partition=0,
              on_delivery=lambda err, msg: errors.append(err))
assert p.flush(20) == 0 and len(errors) == 5 and not any(errors), errors
c = Consumer({'bootstrap.servers':'localhost:9097', 'group.id':'sasl-check',
              'security.protocol':'SASL_PLAINTEXT', 'sasl.mechanism':'PLAIN',
              'sasl.username':'alice', 'sasl.password':'wonderland-42', 'enable.auto.commit':False})
c.assign([TopicPartition('compat-sasl',0,0)])
got = []; deadline = time.monotonic() + 20
while len(got) < 5 and time.monotonic() < deadline:
    m = c.poll(1)
    if m is not None and not m.error(): got.append(m.value())
c.close()
assert got == [f'sasl-{i}'.encode() for i in range(5)], got
for user, password in [('alice','wrong'), ('eve','anything')]:
    auth_errors = []
    bad = Producer({'bootstrap.servers':'localhost:9097', 'security.protocol':'SASL_PLAINTEXT',
                    'sasl.mechanism':'PLAIN', 'sasl.username':user, 'sasl.password':password,
                    'message.timeout.ms':5000, 'error_cb':auth_errors.append})
    delivered = []
    bad.produce('compat-sasl', value='forbidden', on_delivery=lambda err,m: delivered.append(err))
    bad.flush(10)
    assert delivered and all(delivered), delivered
    assert any(e.code() == KafkaError._AUTHENTICATION for e in auth_errors), auth_errors
print('PASS: authenticated produce/consume; wrong password and unknown user rejected by SASL')
PY
