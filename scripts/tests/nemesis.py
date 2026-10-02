#!/usr/bin/env python3
"""Reproducible single-fault schedules that preserve min ISR and Raft quorum."""
import random
import threading


def eligible_victims(partitions, alive, min_isr, cluster_size):
    if min_isr < 1 or cluster_size < 3 or not partitions:
        return []
    if any(p['leader'] not in alive for p in partitions.values()):
        return []
    return [victim for victim in sorted(alive)
            if len(alive - {victim}) >= cluster_size // 2 + 1
            and all(len(set(p['isr']) & (alive - {victim})) >= min_isr
                    for p in partitions.values())]


class Planner:
    REQUIRED = [(role, action) for action in ('kill9', 'pause')
                for role in ('follower', 'leader', 'controller')]

    def __init__(self, seed, min_isr=2, cluster_size=3):
        self.rng = random.Random(seed)
        self.min_isr = min_isr
        self.cluster_size = cluster_size
        self.count = 0

    def next(self, metadata, alive):
        candidates = eligible_victims(metadata['partitions'], alive,
                                      self.min_isr, self.cluster_size)
        if not candidates:
            return None
        role, action = (self.REQUIRED[self.count] if self.count < len(self.REQUIRED)
                        else self.rng.choice(self.REQUIRED))
        leader = metadata['partitions'][0]['leader']
        if role == 'leader':
            candidates = [v for v in candidates if v == leader]
        elif role == 'follower':
            candidates = [v for v in candidates if v != leader]
        else:
            candidates = [v for v in candidates if v == metadata['controller']]
        if not candidates:
            return None
        self.count += 1
        return {'broker': self.rng.choice(candidates), 'role': role, 'action': action,
                'hold_seconds': self.rng.uniform(4, 7)}


class Nemesis(threading.Thread):
    """A driver heals each fault before considering another; errors fail the run.

    ready() must wait for every process to be healthy and every topic partition
    to have the complete ISR. Its snapshot is saved with each proposed fault.
    The workload keeps running while this thread injects, waits, and heals.
    """

    def __init__(self, control, ready, ledger, stop, seed):
        super().__init__(name='nemesis')
        self.control, self.ready, self.ledger, self.stop = control, ready, ledger, stop
        self.planner = Planner(seed)
        self.coverage = set()
        self.error = None

    def run(self):
        try:
            while not self.stop.is_set():
                snapshot = self.ready()
                if self.stop.is_set():
                    break
                fault = self.planner.next(snapshot, {0, 1, 2})
                if fault is None:
                    raise RuntimeError('no safe victim in a healed three-broker cluster')
                self.ledger.append(dict(type='fault', metadata=snapshot, **fault))
                # Even a signal-command error may have changed the process, so
                # healing belongs in finally around both injection and the wait.
                try:
                    self.control(fault['action'], str(fault['broker']))
                    self.coverage.add((fault['role'], fault['action']))
                    self.stop.wait(fault['hold_seconds'])
                finally:
                    healing = 'resume' if fault['action'] == 'pause' else 'restart'
                    self.control(healing, str(fault['broker']))
                    self.ledger.append(dict(type='heal', broker=fault['broker'], action=healing))
                self.ready()
                self.ledger.append(dict(type='settled', broker=fault['broker']))
                self.stop.wait(self.planner.rng.uniform(1, 2))
        except BaseException as exc:
            self.error = exc
            self.stop.set()
