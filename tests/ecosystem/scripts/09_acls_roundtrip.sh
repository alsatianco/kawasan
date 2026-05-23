#!/usr/bin/env bash
# 09_acls_roundtrip.sh: ACL Create + Describe + Delete round-trip.
# Phase 4.2c validates the in-memory ACL store: bindings created via
# CreateAcls show up in DescribeAcls; DeleteAcls removes them and a
# follow-up Describe returns empty.
set -euo pipefail
TARGET="${1:-kawasan}"

python3 - <<'PY'
from kafka.admin import KafkaAdminClient
from kafka.admin.acl_resource import (
    ACL, ACLFilter, ACLOperation, ACLPermissionType,
    ResourcePattern, ResourceType, ACLResourcePatternType,
)
import sys

BROKERS = 'localhost:9092'
TOPIC_NAME = "compat-09-topic"
PRINCIPAL = "User:compat-09"
HOST = "*"

admin = KafkaAdminClient(bootstrap_servers=BROKERS)

# 1) Create an ACL: ALLOW READ for User:compat-09 on TOPIC compat-09-topic
acl = ACL(
    principal=PRINCIPAL,
    host=HOST,
    operation=ACLOperation.READ,
    permission_type=ACLPermissionType.ALLOW,
    resource_pattern=ResourcePattern(
        resource_type=ResourceType.TOPIC,
        resource_name=TOPIC_NAME,
        pattern_type=ACLResourcePatternType.LITERAL,
    ),
)
res = admin.create_acls([acl])
print(f"CreateAcls: {res}")

# 2) Describe: filter on TOPIC + LITERAL + name. We expect 1 match.
fltr = ACLFilter(
    principal=None,
    host=None,
    operation=ACLOperation.ANY,
    permission_type=ACLPermissionType.ANY,
    resource_pattern=ResourcePattern(
        resource_type=ResourceType.TOPIC,
        resource_name=TOPIC_NAME,
        pattern_type=ACLResourcePatternType.LITERAL,
    ),
)
matches = admin.describe_acls(fltr)
print(f"DescribeAcls: matches={matches}")

# kafka-python returns a tuple (ListOfAcls, error). Extract list.
acls_list = matches[0] if isinstance(matches, tuple) else matches
if not acls_list or len(acls_list) < 1:
    print(f"FAIL: expected at least 1 ACL, got {acls_list}")
    sys.exit(1)

# 3) Delete with the same filter — expect 1 removed.
del_res = admin.delete_acls([fltr])
print(f"DeleteAcls: {del_res}")

# 4) Describe again — should be empty.
matches2 = admin.describe_acls(fltr)
acls_list2 = matches2[0] if isinstance(matches2, tuple) else matches2
if acls_list2:
    print(f"FAIL: expected 0 ACLs after delete, got {acls_list2}")
    sys.exit(1)

admin.close()
print("PASS: ACL Create + Describe + Delete + Describe-empty")
PY
