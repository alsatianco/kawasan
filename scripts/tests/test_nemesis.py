"""Safety and coverage fixtures for the M9 fault planner."""
import random
import unittest

from nemesis import Planner, eligible_victims


def metadata():
    return {'controller': 2, 'partitions': {
        0: {'leader': 0, 'isr': [0, 1, 2]},
        1: {'leader': 1, 'isr': [0, 1, 2]},
        2: {'leader': 2, 'isr': [0, 1, 2]}}}


class NemesisTest(unittest.TestCase):
    def test_one_fault_preserves_min_isr_and_raft_majority(self):
        self.assertEqual(eligible_victims(metadata()['partitions'], {0, 1, 2}, 2, 3), [0, 1, 2])
        self.assertEqual(eligible_victims(metadata()['partitions'], {0, 2}, 2, 3), [])

    def test_stale_isr_does_not_count_an_unavailable_replica(self):
        parts = metadata()['partitions']
        parts[1]['isr'] = [0, 1]
        self.assertEqual(eligible_victims(parts, {0, 1, 2}, 2, 3), [2])
        self.assertEqual(eligible_victims(parts, {0, 2}, 2, 3), [])

    def test_raft_majority_is_independent_of_min_isr(self):
        self.assertEqual(eligible_victims(metadata()['partitions'], {0, 2}, 1, 3), [])

    def test_offline_partition_disallows_faults(self):
        parts = metadata()['partitions']; parts[0]['leader'] = -1
        self.assertEqual(eligible_victims(parts, {0, 1, 2}, 2, 3), [])

    def test_all_six_roles_and_signals_are_required_before_randomization(self):
        planner = Planner(seed=42)
        faults = [planner.next(metadata(), {0, 1, 2}) for _ in range(6)]
        self.assertEqual({(f['role'], f['action']) for f in faults}, {
            (role, action) for role in ['leader', 'follower', 'controller']
            for action in ['kill9', 'pause']})
        for fault in faults:
            if fault['role'] == 'follower': self.assertNotEqual(fault['broker'], 0)
            if fault['role'] == 'leader': self.assertEqual(fault['broker'], 0)
            if fault['role'] == 'controller': self.assertEqual(fault['broker'], 2)

    def test_seed_reproduces_schedule(self):
        left, right = Planner(seed=91), Planner(seed=91)
        self.assertEqual([left.next(metadata(), {0, 1, 2}) for _ in range(20)],
                         [right.next(metadata(), {0, 1, 2}) for _ in range(20)])

    def test_refusing_fault_does_not_skip_required_coverage(self):
        planner = Planner(seed=0)
        self.assertIsNone(planner.next(metadata(), {0, 2}))
        self.assertEqual(planner.next(metadata(), {0, 1, 2})['role'], 'follower')


if __name__ == '__main__':
    unittest.main()
