#!/usr/bin/env python3
"""Check retained broker request evidence after a successful candidate matrix."""
import json
from pathlib import Path
import re
import sys

CAPS = {
    "4.x": {0: 11, 1: 13, 2: 8, 9: 9, 21: 2, 36: 2, 24: 3, 25: 3, 26: 3, 28: 3},
    "3.x": {0: 9, 1: 12, 2: 7, 9: 8, 21: 0, 36: 1, 24: 3, 25: 3, 26: 3, 28: 3},
}


def observed_versions(log):
    observed = {}
    for api, version in re.findall(r"request api_key=(\d+) version=(\d+)", log):
        observed.setdefault(int(api), set()).add(int(version))
    return observed


def verify(observed, profile, compatibility):
    if compatibility not in CAPS:
        return [f"unknown API compatibility profile: {compatibility}"]
    caps = CAPS[compatibility]
    errors = []
    for api, versions in observed.items():
        if api in caps and any(version > caps[api] for version in versions):
            errors.append(f"API {api} requests exceeded profile maximum {caps[api]}")
    required = {0, 1}
    if profile.startswith("java-"):
        required |= {2, 9, 24, 25, 26, 28}
    for api in sorted(required):
        if not observed.get(api):
            errors.append(f"missing request evidence for API {api}")
        elif profile.startswith("java-") and caps[api] not in observed[api]:
            errors.append(f"Java did not negotiate planned API {api} version {caps[api]}")
    return errors


def main():
    profile, compatibility, path = sys.argv[1:]
    observed = observed_versions(Path(path).read_text())
    errors = verify(observed, profile, compatibility)
    print(json.dumps({"profile": profile, "compatibility": compatibility,
                      "observed": {str(k): sorted(v) for k, v in sorted(observed.items())},
                      "errors": errors}, indent=2))
    if errors:
        print("FAIL: " + "; ".join(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
