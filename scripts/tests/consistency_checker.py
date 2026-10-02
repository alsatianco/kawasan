#!/usr/bin/env python3
"""M9 durable acknowledgement ledger and independent consistency invariants.

Client/process driving is added below; these checks have no Kafka dependency so
corrupt histories can be tested independently of the broker and scanner.
"""
import argparse
import json
import os
import threading
from collections import Counter
from pathlib import Path


class Violation(RuntimeError):
    """A consistency invariant was violated (never a retriable client error)."""


class Ledger:
    """Create a new evidence file; flush and fsync each event before returning."""

    def __init__(self, path):
        self.path = Path(path)
        self.file = self.path.open('x', encoding='utf-8')
        self.lock = threading.Lock()
        directory = os.open(self.path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def append(self, event):
        with self.lock:
            self.file.write(json.dumps(event, sort_keys=True) + '\n')
            self.file.flush()
            os.fsync(self.file.fileno())

    def close(self):
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @staticmethod
    def load(path):
        with Path(path).open(encoding='utf-8') as stream:
            return [json.loads(line) for line in stream]


def transactions(events):
    states = {}
    for event in events:
        if event['type'] == 'txn':
            states.setdefault(event['id'], {}).update(event)
    return states


def verify_records(events, records, end_offsets, transactional=False):
    """I1 exact acked offset/value; I2 ordered scans and a contiguous fresh log.

    Ambiguous failed writes may exist in the log. Control/aborted transaction
    offsets are legal gaps and are checked by I4 instead of requiring contiguity.
    The input is an ordered scan, deliberately not a dict that hides duplicates.
    """
    commits = {key for key, value in transactions(events).items()
               if value['decision'] == 'commit'}
    acked = {}
    for event in events:
        if event['type'] != 'ack':
            continue
        if transactional and event.get('txn') is not None and event['txn'] not in commits:
            continue
        point = (event['partition'], event['offset'])
        payload = (event['topic'], event['key'], event['value'])
        if point in acked and acked[point] != payload:
            raise Violation(f'I2: conflicting acknowledgements at {point}')
        acked[point] = payload

    actual = {}
    offsets = {partition: [] for partition in end_offsets}
    for record in records:
        point = (record['partition'], record['offset'])
        if point in actual:
            raise Violation(f'I2: repeated scan offset {point}')
        actual[point] = (record['topic'], record['key'], record['value'])
        offsets.setdefault(record['partition'], []).append(record['offset'])
    for point, payload in acked.items():
        if actual.get(point) != payload:
            raise Violation(f'I1: acknowledged record missing or changed at {point}: {payload}')
    for partition, seen in offsets.items():
        if any(right <= left for left, right in zip(seen, seen[1:])):
            raise Violation(f'I2: scan offsets regress in partition {partition}: {seen[:10]}')
        end = end_offsets.get(partition)
        if end is None or any(offset < 0 or offset >= end for offset in seen):
            raise Violation(f'I2: scan outside committed log boundary in partition {partition}')
        if not transactional and (len(seen) != end or any(o != i for i, o in enumerate(seen))):
            raise Violation(f'I2: gap in partition {partition}: {len(seen)} records, end={end}')
    return {'acknowledged': len(acked), 'scanned': len(records)}


def verify_watermarks(events, partitions):
    """I3 successful committed ListOffsets observations may never regress."""
    previous = {}
    counts = Counter()
    for event in events:
        if event['type'] != 'watermark':
            continue
        partition, offset = event['partition'], event['offset']
        if offset < 0 or offset < previous.get(partition, 0):
            raise Violation(f'I3: committed offset regressed on {partition}: '
                            f'{previous.get(partition)} -> {offset}')
        previous[partition] = offset
        counts[partition] += 1
    for partition in partitions:
        if counts[partition] < 2:
            raise Violation(f'I3: insufficient committed-offset observations for {partition}')


def verify_transactions(events, records, group_offset):
    """I4 each transaction is all-or-nothing and group offsets match visibility."""
    visible = Counter(record['key'] for record in records)
    expected_group_offset = -1
    for txn_id, txn in transactions(events).items():
        keys = txn['keys']
        seen = sum(visible[key] > 0 for key in keys)
        if any(visible[key] > 1 for key in keys) or seen not in (0, len(keys)):
            raise Violation(f'I4: partial or duplicate visibility for transaction {txn_id}')
        if txn['decision'] == 'commit' and seen != len(keys):
            raise Violation(f'I4: committed transaction {txn_id} is missing')
        if txn['decision'] == 'abort' and seen:
            raise Violation(f'I4: aborted transaction {txn_id} is visible')
        if seen:
            expected_group_offset = max(expected_group_offset, txn['input_offset'])
    if group_offset != expected_group_offset:
        raise Violation(f'I4: staged group offset {group_offset} disagrees with visible '
                        f'transactions ({expected_group_offset})')


def verify_artifacts(directory):
    directory = Path(directory)
    events = Ledger.load(directory / 'ledger.jsonl')
    scan = json.loads((directory / 'scan.json').read_text())
    ends = {int(partition): offset for partition, offset in scan['end_offsets'].items()}
    result = verify_records(events, scan['records'], ends, scan.get('transactional', False))
    verify_watermarks(events, ends)
    if scan.get('transactional'):
        verify_transactions(events, scan['records'], scan['group_offset'])
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify-only', type=Path, required=True,
                        help='recheck a saved ledger.jsonl and scan.json without a broker')
    args = parser.parse_args()
    print(json.dumps(verify_artifacts(args.verify_only), sort_keys=True))
