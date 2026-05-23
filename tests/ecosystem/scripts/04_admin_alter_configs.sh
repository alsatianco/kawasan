#!/usr/bin/env bash
# 04_admin_alter_configs.sh: admin-API smoke. AlterConfigs (33) and
# IncrementalAlterConfigs (44) are Phase 4 targets; expected to fail on
# Kawasan today.
set -euo pipefail
TARGET="${1:-kawasan}"
TOPIC="compat-04-$$"

python3 - <<PY
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
a = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='compat-04')
a.create_topics([NewTopic('$TOPIC', num_partitions=1, replication_factor=1)])
res = a.alter_configs([
    ConfigResource(ConfigResourceType.TOPIC, '$TOPIC', {'retention.ms': '60000'})
])
print('alter result:', res)
a.close()
print('PASS')
PY
