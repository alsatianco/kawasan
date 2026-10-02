"""Independent fixtures for the M9 checker: each corruption must be rejected."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from consistency_checker import Ledger, Violation, verify_records, verify_watermarks, verify_transactions


def record(offset, key=None, partition=0):
    key = key or f'run:{offset}'
    return dict(topic='t', partition=partition, offset=offset, key=key, value=key)


def ack(offset, key=None, partition=0, txn=None):
    return dict(type='ack', txn=txn, **record(offset, key, partition))


class CheckerTest(unittest.TestCase):
    def test_valid_with_ambiguous_unacknowledged_write(self):
        verify_records([ack(0), ack(2)], [record(i) for i in range(3)], {0: 3})

    def test_acked_record_hidden_by_seeded_high_watermark(self):
        with self.assertRaisesRegex(Violation, 'I1'):
            verify_records([ack(0), ack(1)], [record(0)], {0: 1})

    def test_wrong_value_or_offset(self):
        for records in [[dict(record(0), value='wrong')], [record(1, 'run:0')]]:
            with self.subTest(records=records), self.assertRaisesRegex(Violation, 'I1'):
                verify_records([ack(0)], records, {0: 2})

    def test_gap_even_without_ack_for_missing_offset(self):
        with self.assertRaisesRegex(Violation, 'I2'):
            verify_records([ack(0), ack(2)], [record(0), record(2)], {0: 3})

    def test_scan_offsets_cannot_regress_or_repeat(self):
        for offsets in [[1, 0], [0, 0]]:
            with self.subTest(offsets=offsets), self.assertRaisesRegex(Violation, 'I2'):
                verify_records([], [record(i) for i in offsets], {0: 2})

    def test_ack_collision_is_not_hidden_by_dictionary(self):
        with self.assertRaisesRegex(Violation, 'I2'):
            verify_records([ack(0), ack(0, 'other')], [record(0)], {0: 1})

    def test_watermarks_never_regress_across_leadership(self):
        events = [dict(type='watermark', partition=0, offset=o) for o in [2, 4, 3]]
        with self.assertRaisesRegex(Violation, 'I3'):
            verify_watermarks(events, [0])

    def test_missing_observations_cannot_pass(self):
        with self.assertRaisesRegex(Violation, 'I3'):
            verify_watermarks([], [0])

    def test_each_partition_is_checked_independently(self):
        events = [dict(type='watermark', partition=p, offset=o)
                  for p, o in [(0, 4), (1, 2), (0, 5), (1, 3)]]
        verify_watermarks(events, [0, 1])

    def test_transaction_control_offsets_are_legal_gaps(self):
        verify_records([ack(0, txn='x')], [record(0)], {0: 3}, transactional=True)

    def test_transaction_atomicity_and_group_offsets(self):
        txns = [dict(type='txn', id='x', keys=['a', 'b'], decision='commit', input_offset=2),
                dict(type='txn', id='y', keys=['c', 'd'], decision='abort', input_offset=4)]
        verify_transactions(txns, [record(0, 'a'), record(1, 'b')], 2)
        for records, group_offset in [([record(0, 'a')], 2),
                                      ([record(0, 'a'), record(1, 'b'), record(3, 'c')], 2),
                                      ([record(0, 'a'), record(1, 'b')], 4)]:
            with self.subTest(records=records), self.assertRaisesRegex(Violation, 'I4'):
                verify_transactions(txns, records, group_offset)

    def test_unknown_commit_must_be_all_or_nothing(self):
        events = [dict(type='txn', id='x', keys=['a', 'b'], decision='unknown', input_offset=2)]
        verify_transactions(events, [], -1)
        verify_transactions(events, [record(0, 'a'), record(1, 'b')], 2)
        with self.assertRaisesRegex(Violation, 'I4'):
            verify_transactions(events, [record(0, 'a')], 2)

    def test_ledger_fsyncs_every_event_and_reloads_exactly(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'ledger.jsonl'
            with patch('consistency_checker.os.fsync') as sync:
                with Ledger(path) as ledger:
                    before = sync.call_count
                    ledger.append(ack(0)); ledger.append(ack(1))
                    self.assertEqual(sync.call_count, before + 2)
            self.assertEqual(Ledger.load(path), [ack(0), ack(1)])

    def test_corrupt_ledger_is_not_silently_truncated(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'ledger.jsonl'
            path.write_text(json.dumps(ack(0)) + '\n{"type":')
            with self.assertRaises((ValueError, Violation)):
                Ledger.load(path)


if __name__ == '__main__':
    unittest.main()
