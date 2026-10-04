#!/usr/bin/env bash
set -euo pipefail
cd /work
mvn -B -q -Dkafka.version="$CLIENT_VERSION" package dependency:copy-dependencies
java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp 'target/classes:target/dependency/*' ClientMatrix
