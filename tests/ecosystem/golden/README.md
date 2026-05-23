# Golden Wire Fixtures (Kafka 4.2)

**0B.6**: This directory will hold raw response bytes captured from a
real Apache Kafka 4.2.0 broker for every API/version pair Kawasan
advertises. The compat-smoke job will replay the matching request and
diff Kawasan's response bytes against the golden — catching wire-format
regressions that the smoke scripts above might not surface.

## How fixtures are produced

A capture helper (planned, Phase 1) will run against the oracle
docker-compose stack and emit fixtures like:

```
golden/
  ApiVersions-v3.req.bin       (request bytes)
  ApiVersions-v3.resp.bin      (response bytes)
  Metadata-v9.req.bin
  Metadata-v9.resp.bin
  ...
```

Naming convention: `{ApiName}-v{version}.{req|resp}.bin`.

## Why these aren't here yet

Capturing fixtures requires the Kafka oracle stack to be runnable on the
host doing the recording. The plumbing (`docker-compose.kafka.yml` and
the run_all.sh orchestrator) shipped in 0B.2 / 0B.3; the capture script
is a Phase 1 deliverable. The directory exists now so Phase 1 work has
an obvious home for its outputs.
