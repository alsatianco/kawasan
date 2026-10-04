"""Fail closed on interrupted runs, skips, unknown failures and stale allowlist rows."""
import argparse
import json
from pathlib import Path

JAVA_CASES = {"admin-topics", "admin-configs", "admin-cluster", "admin-groups", "admin-log-dirs", "admin-acls", "produce-consume", "transactions", "eos-offsets", "producer-fencing", "api-parity"}
RDKAFKA_CASES = {"admin-topics", "produce-consume", "transactions", "eos-offsets"}
PROFILES = {"java-3.9.1": JAVA_CASES, "java-4.2.0": JAVA_CASES,
            "rdkafka-2.8.0": RDKAFKA_CASES, "rdkafka-2.15.1": RDKAFKA_CASES,
            "python": {"produce-consume"}}


def validate(profile, target, rows, allowlist):
    expected = PROFILES[profile]
    if target not in {"kafka", "kawasan"}:
        raise ValueError("unknown target")
    names = [r.get("case") for r in rows]
    if set(names) != expected or len(names) != len(expected):
        raise ValueError(f"incomplete/duplicate/unknown cases: expected {sorted(expected)}, got {names}")
    allowed = {}
    for row in allowlist:
        if len(row) != 5 or row[0] not in PROFILES or row[1] not in PROFILES[row[0]] or not all(row):
            raise ValueError(f"invalid allowlist row: {row}")
        key = row[:2]
        if key in allowed:
            raise ValueError(f"duplicate allowlist row: {key}")
        allowed[key] = row[2]
    failures = []
    for r in rows:
        signature = allowed.get((profile, r["case"])) if target == "kawasan" else None
        if r.get("status") == "PASS":
            if signature:
                failures.append(f"{r['case']}: unexpected pass; remove allowlist entry")
        elif r.get("status") == "FAIL":
            if not signature or signature not in r.get("error", ""):
                failures.append(f"{r['case']}: {r.get('error')}")
        else:
            failures.append(f"{r['case']}: skips/unknown status are not passes")
    if failures:
        raise ValueError("\n".join(failures))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("profile", choices=PROFILES)
    p.add_argument("target", choices=("kafka", "kawasan"))
    p.add_argument("results", type=Path)
    p.add_argument("--allowlist", type=Path, default=Path(__file__).with_name("known_failures.txt"))
    args = p.parse_args()
    rows = [json.loads(line) for line in args.results.read_text().splitlines()]
    allowlist = [tuple(line.split("\t")) for line in args.allowlist.read_text().splitlines() if line and not line.startswith("#")]
    validate(args.profile, args.target, rows, allowlist)
    passes = sum(r["status"] == "PASS" for r in rows)
    print(f"{args.profile} {args.target}: {passes}/{len(rows)} passes, {len(rows)-passes} known failures, zero skips")

if __name__ == "__main__":
    main()
