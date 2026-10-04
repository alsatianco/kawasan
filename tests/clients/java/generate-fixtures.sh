#!/usr/bin/env bash
# Run Kafka's own generated codecs, keeping Maven output separate from the JSON.
set -euo pipefail
SOURCE="$(cd "$(dirname "$0")" && pwd)"
OUTPUT="${1:?Usage: generate-fixtures.sh OUTPUT_JSON}"
WORK="$(mktemp -d /tmp/kawasan-wire-fixtures-XXXXXX)"
mkdir -p "$WORK/java"
cp -R "$SOURCE/." "$WORK/java/"
docker run --rm -v "$WORK/java:/work" -v kawasan-client-maven:/root/.m2 -w /work \
    maven:3.9.9-eclipse-temurin-21 \
    bash -c 'mvn -B -q -Pwire-fixtures -Dkafka.version=4.2.0 package dependency:copy-dependencies > /work/build.log 2>&1 && java -cp "target/classes:target/dependency/*" WireFixtures' \
    > "$WORK/fixtures.json" 2> "$WORK/run.log"
python3 -m json.tool "$WORK/fixtures.json" > "$OUTPUT"
echo "Kafka 4.2 fixtures written to $OUTPUT; build evidence at $WORK" >&2
