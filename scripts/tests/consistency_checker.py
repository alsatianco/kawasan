#!/usr/bin/env python3
"""M9 durable acknowledgement ledger and independent consistency invariants.

Pure checks have no Kafka dependency. Live modes use librdkafka and the local
process harness; every run retains broker data, a ledger, scan, and summary.
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
    keys = set()
    offsets = {partition: [] for partition in end_offsets}
    for record in records:
        point = (record['partition'], record['offset'])
        if point in actual:
            raise Violation(f'I2: repeated scan offset {point}')
        identity = (record['topic'], record['key'])
        if identity in keys:
            raise Violation(f'I2: repeated write key {identity} at distinct offsets')
        keys.add(identity)
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



class Cluster:
    """Own only fresh local harness processes and retain all evidence on exit."""

    def __init__(self, directory, single_node, broker_bin, port_base):
        self.root = Path(__file__).resolve().parents[2]
        self.directory = Path(directory).resolve()
        self.n = 1 if single_node else 3
        self.port_base = port_base
        self.broker_bin = Path(broker_bin or self.root / 'build/tools/kawasan-broker').resolve()
        if not self.broker_bin.is_file():
            raise RuntimeError(f'broker binary missing: {self.broker_bin}')
        self.env = dict(os.environ, BASE=str(self.directory), N=str(self.n), KEEP_DATA='1',
                        BROKER_BIN=str(self.broker_bin), KAFKA_BASE=str(port_base),
                        RAFT_BASE=str(port_base + 1), MON_BASE=str(port_base + 2),
                        MIN_ISR=str(1 if single_node else 2), LAG_MS='1500', LIVENESS_MS='2500',
                        LOG_LEVEL='info')
        self.bootstrap = ','.join(f'127.0.0.1:{port_base + i * 100}' for i in range(self.n))
        self.client_config = {'bootstrap.servers': self.bootstrap, 'socket.timeout.ms': 5000,
                              'topic.metadata.refresh.interval.ms': 1000, 'log_level': 0}

    def control(self, *args):
        import subprocess
        result = subprocess.run(['bash', str(self.root / 'scripts/tests/cluster_harness.sh'), *args],
                                env=self.env, cwd=self.root, capture_output=True, text=True,
                                timeout=45)
        with (self.directory.parent / 'harness.log').open('a') as stream:
            stream.write(result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError(f'harness {args} failed: {result.stdout} {result.stderr}')

    def start(self):
        import socket
        # A failed bind must not leave a test talking to an unrelated broker.
        for broker in range(self.n):
            for extra in range(3):
                with socket.socket() as sock:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock.bind(('127.0.0.1', self.port_base + broker * 100 + extra))
        self.control('up')
        self.wait_ready()

    def wait_ready(self, topic=None, timeout=60):
        import subprocess
        import time
        import urllib.request
        from confluent_kafka.admin import AdminClient
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                for broker in range(self.n):
                    pid = int((self.directory / f'broker-{broker}/broker.pid').read_text())
                    cmd = subprocess.check_output(['ps', '-ww', '-p', str(pid), '-o', 'args='],
                                                  text=True)
                    if str(self.broker_bin) not in cmd or str(self.directory) not in cmd:
                        raise RuntimeError('stale/unrelated broker PID')
                    url = f'http://127.0.0.1:{self.port_base + broker * 100 + 2}/readiness'
                    with urllib.request.urlopen(url, timeout=1) as response:
                        if response.status != 200: raise RuntimeError('broker not ready')
                md = AdminClient(self.client_config).list_topics(topic=topic, timeout=3)
                if topic is None:
                    return md
                entry = md.topics.get(topic)
                if entry is None or entry.error:
                    raise RuntimeError(f'topic metadata unavailable: {entry}')
                parts = {p: {'leader': pm.leader, 'isr': list(pm.isrs)}
                         for p, pm in entry.partitions.items()}
                if md.controller_id < 0 or md.controller_id >= self.n:
                    raise RuntimeError('controller not elected')
                if all(pm['leader'] >= 0 and sorted(pm['isr']) == list(range(self.n))
                       for pm in parts.values()):
                    return {'controller': md.controller_id, 'partitions': parts}
                last = parts
            except Exception as exc:
                last = exc
            time.sleep(.2)
        raise RuntimeError(f'cluster failed to heal/readiness within {timeout}s: {last}')

    def create_topic(self, topic, partitions=3):
        import time
        from confluent_kafka import KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic
        for attempt in range(20):
            try:
                md = AdminClient(self.client_config).list_topics(timeout=3)
                config = dict(self.client_config,
                              **{'bootstrap.servers': f'127.0.0.1:{self.port_base + md.controller_id * 100}'})
                controller_admin = AdminClient(config)
                controller_admin.create_topics([NewTopic(
                    topic, num_partitions=partitions, replication_factor=self.n,
                    config={'min.insync.replicas': str(1 if self.n == 1 else 2),
                            'cleanup.policy': 'delete', 'retention.ms': '-1'})])[topic].result(10)
                return self.wait_ready(topic)
            except KafkaException as exc:
                if 'NOT_CONTROLLER' not in str(exc) or attempt == 19:
                    raise
                time.sleep(.3)


def observe_offsets(admin, topic, partitions, ledger):
    from confluent_kafka import IsolationLevel, TopicPartition, KafkaException
    from confluent_kafka.admin import OffsetSpec
    futures = admin.list_offsets({TopicPartition(topic, p): OffsetSpec.latest() for p in partitions},
                                 isolation_level=IsolationLevel.READ_COMMITTED, request_timeout=3)
    result = {}
    for tp, future in futures.items():
        try:
            offset = future.result(5).offset
        except KafkaException as exc:
            ledger.append(dict(type='unavailable', partition=tp.partition, error=str(exc)))
            continue
        if offset < 0:
            # librdkafka can return OFFSET_INVALID during leader discovery.
            # This is no committed-offset observation; preserve it as unavailable.
            ledger.append(dict(type='unavailable', partition=tp.partition,
                               error='invalid client offset', offset=offset))
            continue
        result[tp.partition] = offset
        ledger.append(dict(type='watermark', partition=tp.partition, offset=offset))
    return result


def scan_output(cluster, topic, ends, timeout=45):
    import time
    import uuid
    from confluent_kafka import Consumer, KafkaError, TopicPartition
    consumer = Consumer(dict(cluster.client_config, **{
        'group.id': 'm9-scan-' + uuid.uuid4().hex, 'enable.auto.commit': False,
        'enable.partition.eof': True, 'isolation.level': 'read_committed',
        'auto.offset.reset': 'error'}))
    consumer.assign([TopicPartition(topic, p, 0) for p in ends])
    records, finished = [], set()
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline and len(finished) != len(ends):
            message = consumer.poll(.2)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    finished.add(message.partition())
                    continue
                raise RuntimeError(f'healed scan failed: {message.error()}')
            records.append(dict(topic=message.topic(), partition=message.partition(),
                                offset=message.offset(), key=message.key().decode(),
                                value=message.value().decode()))
        if len(finished) != len(ends):
            raise RuntimeError(f'full scan timed out: EOF partitions {sorted(finished)}')
        return records
    finally:
        consumer.close()


def save_json(path, value):
    temporary = Path(str(path) + '.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(Path(path).parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def run_writes(cluster, topic, duration, ledger, seed, no_faults=False):
    import time
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    from nemesis import Nemesis, Planner
    run_id = topic
    partitions = list(range(3))
    stop = threading.Event()
    errors = []
    admin = AdminClient(cluster.client_config)
    # A separate observer keeps sampling while producer callbacks fsync and
    # while the nemesis waits for ISR healing; transient RPC errors stay in evidence.
    def observer():
        try:
            while not stop.is_set():
                observe_offsets(admin, topic, partitions, ledger)
                stop.wait(.5)
        except BaseException as exc:
            errors.append(exc)
            stop.set()
    sampler = threading.Thread(target=observer, name='committed-offset-observer')
    nemesis = None
    if cluster.n == 3 and not no_faults:
        nemesis = Nemesis(cluster.control, lambda: cluster.wait_ready(topic), ledger, stop, seed)
    producer = Producer(dict(cluster.client_config, **{
        'acks': 'all', 'enable.idempotence': True, 'message.timeout.ms': 15000,
        'request.timeout.ms': 3000, 'max.in.flight.requests.per.connection': 1,
        'batch.num.messages': 1, 'linger.ms': 0}))
    successes, failures = Counter(), Counter()
    def callback(key, partition):
        def delivered(error, message):
            if error:
                failures[partition] += 1
                ledger.append(dict(type='failed_write', key=key, partition=partition,
                                   error=str(error)))
            else:
                ledger.append(dict(type='ack', topic=topic, partition=message.partition(),
                                   offset=message.offset(), key=key, value=key, txn=None))
                successes[partition] += 1
        return delivered
    seq = 0
    next_kill = time.monotonic() + 2
    kills = 0
    # Faults must start with acknowledged data on every partition.
    for partition in partitions:
        for _ in range(10):
            key = f'{run_id}:{seq}'; seq += 1
            producer.produce(topic, key=key, value=key, partition=partition,
                             on_delivery=callback(key, partition))
    if producer.flush(30) or any(successes[p] < 10 for p in partitions):
        raise RuntimeError('baseline acks=all writes failed before nemesis')
    observe_offsets(admin, topic, partitions, ledger)
    sampler.start()
    if nemesis: nemesis.start()
    deadline = time.monotonic() + duration
    try:
        while time.monotonic() < deadline and not stop.is_set():
            for partition in partitions:
                key = f'{run_id}:{seq}'; seq += 1
                try:
                    producer.produce(topic, key=key, value=key, partition=partition,
                                     on_delivery=callback(key, partition))
                except BufferError:
                    producer.poll(.1)
                    # Never silently drop an intended write from workload accounting.
                    producer.produce(topic, key=key, value=key, partition=partition,
                                     on_delivery=callback(key, partition))
            producer.poll(.01)
            stop.wait(.05)
            if cluster.n == 1 and not no_faults and time.monotonic() >= next_kill:
                # Deliberately kill with asynchronous records still outstanding.
                ledger.append(dict(type='fault', broker=0, action='kill9', role='single-node'))
                cluster.control('kill9', '0')
                cluster.control('restart', '0')
                cluster.wait_ready(topic)
                ledger.append(dict(type='heal', broker=0, action='restart'))
                kills += 1
                next_kill = time.monotonic() + 4
    finally:
        stop.set()
        sampler.join(15)
        if sampler.is_alive(): raise RuntimeError('offset observer failed to stop')
        if nemesis:
            nemesis.join(90)
            if nemesis.is_alive(): raise RuntimeError('nemesis failed to stop/heal')
        cluster.wait_ready(topic)
        import sys
        # Avoid invoking C callbacks while a Python/C client exception is active;
        # preserve that original failure instead of masking it during cleanup.
        if sys.exc_info()[0] is None and producer.flush(30):
            raise RuntimeError('producer delivery callbacks did not drain')
    if errors: raise errors[0]
    if nemesis and nemesis.error: raise nemesis.error
    if any(successes[p] == 0 for p in partitions):
        raise RuntimeError(f'no successful acknowledgement on every partition: {successes}')
    if cluster.n == 1 and not no_faults and kills == 0:
        raise RuntimeError('duration too short to exercise a single-node SIGKILL')
    if nemesis and nemesis.coverage != set(Planner.REQUIRED):
        raise RuntimeError(f'fault coverage incomplete: {nemesis.coverage}')
    # Require a fresh sample before faults and after healing, even on very short runs.
    ends = observe_offsets(admin, topic, partitions, ledger)
    if len(ends) != len(partitions): raise RuntimeError('final committed offsets unavailable')
    return ends, {'successful_deliveries': dict(successes), 'failed_deliveries': dict(failures),
                  'fault_coverage': sorted(nemesis.coverage) if nemesis else [], 'kills': kills}


def run_transactions(cluster, topic, duration, ledger):
    """Single-node I4: commit, abort and SIGKILL after staging group offsets.

    RF=1 coordinators deliberately stay single-node until M10. Reinitializing
    the same transactional ID after a crash resolves the unfinished transaction.
    """
    import time
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient
    input_topic = topic + '-input'
    cluster.create_topic(input_topic, partitions=1)
    seed = Producer(cluster.client_config)
    delivered = []
    for i in range(1000):
        seed.produce(input_topic, value=str(i), partition=0,
                     on_delivery=lambda error, message: delivered.append(error))
    if seed.flush(20) or len(delivered) != 1000 or any(delivered):
        raise RuntimeError('transaction input seeding failed')
    consumer = Consumer(dict(cluster.client_config, **{
        'group.id': topic + '-group', 'enable.auto.commit': False,
        'isolation.level': 'read_committed'}))
    consumer.assign([TopicPartition(input_topic, 0, 0)])
    config = dict(cluster.client_config, **{
        'transactional.id': topic + '-producer', 'transaction.timeout.ms': 60000,
        'enable.idempotence': True, 'acks': 'all', 'batch.num.messages': 1, 'linger.ms': 0})
    producer = Producer(config)
    producer.init_transactions(20)
    admin = AdminClient(cluster.client_config)
    counts = Counter()
    deadline = time.monotonic() + duration
    cycle = 0
    def committed():
        offset = consumer.committed([TopicPartition(input_topic, 0)], timeout=5)[0].offset
        return max(-1, offset)
    try:
        observe_offsets(admin, topic, range(3), ledger)
        while time.monotonic() < deadline and cycle < 1000:
            baseline = committed()
            txn = f'{topic}:txn:{cycle}'
            keys = [f'{txn}:{p}:{i}' for p in range(3) for i in range(2)]
            intent = dict(type='txn', id=txn, keys=keys, decision='unknown', input_offset=cycle + 1)
            ledger.append(intent)
            producer.begin_transaction()
            deliveries = []
            def callback(key):
                def delivered(error, message):
                    deliveries.append(error)
                    if error is None:
                        ledger.append(dict(type='ack', topic=topic, partition=message.partition(),
                                           offset=message.offset(), key=key, value=key, txn=txn))
                return delivered
            for index, key in enumerate(keys):
                producer.produce(topic, key=key, value=key, partition=index // 2,
                                 on_delivery=callback(key))
            if producer.flush(20) or len(deliveries) != len(keys) or any(deliveries):
                raise RuntimeError(f'transaction data delivery failed: {deliveries}')
            producer.send_offsets_to_transaction([TopicPartition(input_topic, 0, cycle + 1)],
                                                 consumer.consumer_group_metadata(), 10)
            if committed() != baseline:
                raise Violation('I4: staged offsets became visible before transaction completion')
            phase = cycle % 3
            if phase == 0:
                producer.commit_transaction(20)
                decision = 'commit'
                expected = cycle + 1
            elif phase == 1:
                producer.abort_transaction(20)
                decision = 'abort'
                expected = baseline
            else:
                ledger.append(dict(type='fault', broker=0, action='kill9', role='transaction'))
                cluster.control('kill9', '0')
                cluster.control('restart', '0')
                cluster.wait_ready(topic)
                ledger.append(dict(type='heal', broker=0, action='restart'))
                # Destruction of the old client cannot cleanly abort a dead broker.
                producer = Producer(config)
                producer.init_transactions(20)
                consumer.close()
                consumer = Consumer(dict(cluster.client_config, **{
                    'group.id': topic + '-group', 'enable.auto.commit': False,
                    'isolation.level': 'read_committed'}))
                consumer.assign([TopicPartition(input_topic, 0, 0)])
                decision = 'abort'
                expected = baseline
                counts['crash'] += 1
            ledger.append(dict(intent, decision=decision))
            actual = committed()
            ledger.append(dict(type='group_offset', offset=actual, txn=txn))
            if actual != expected:
                raise Violation(f'I4: group offset after {decision} is {actual}, expected {expected}')
            counts[decision] += 1
            observe_offsets(admin, topic, range(3), ledger)
            cycle += 1
            time.sleep(.05)
        if not all(counts[name] for name in ('commit', 'abort', 'crash')):
            raise RuntimeError(f'transaction coverage incomplete: {counts}')
        ends = observe_offsets(admin, topic, range(3), ledger)
        if len(ends) != 3: raise RuntimeError('transaction final offsets unavailable')
        return ends, {'transaction_coverage': dict(counts)}, committed()
    finally:
        consumer.close()


def run_live(args):
    import datetime
    import hashlib
    import subprocess
    import tempfile
    import time
    import uuid
    directory = (args.artifacts or Path(tempfile.mkdtemp(prefix='kawasan-m9-'))).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / 'ledger.jsonl').exists() or (directory / 'cluster').exists():
        raise RuntimeError(f'evidence directory already used: {directory}')
    print(f'Evidence: {directory}', flush=True)
    cluster = Cluster(directory / 'cluster', args.single_node, args.broker_bin, args.port_base)
    summary = {'status': 'failed', 'seed': args.seed, 'single_node': args.single_node,
               'duration_seconds': args.duration, 'transactions': args.transactions,
               'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
               'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=cluster.root,
                                                   text=True).strip(),
               'broker_sha256': hashlib.sha256(cluster.broker_bin.read_bytes()).hexdigest(),
               'checker_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    started = time.monotonic()
    try:
        with Ledger(directory / 'ledger.jsonl') as ledger:
            ledger.append(dict(type='run', **summary))
            cluster.start()
            topic = 'm9-' + uuid.uuid4().hex
            cluster.create_topic(topic)
            if args.transactions:
                ends, stats, group_offset = run_transactions(cluster, topic, args.duration, ledger)
            else:
                ends, stats = run_writes(cluster, topic, args.duration, ledger, args.seed, args.no_faults)
            summary.update(stats)
            records = scan_output(cluster, topic, ends)
            scan = {'records': records, 'end_offsets': ends, 'transactional': args.transactions}
            if args.transactions: scan['group_offset'] = group_offset
            save_json(directory / 'scan.json', scan)
        result = verify_artifacts(directory)
        summary.update(stats, **result, status='passed')
        print(f'PASS: {result}; faults={stats}', flush=True)
    except BaseException as exc:
        summary['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        try:
            # start() can fail before ports were acquired: only our matching PIDs
            # are signalled, and KEEP_DATA retains broker logs/RocksDB forensics.
            if cluster.directory.exists(): cluster.control('down')
        except BaseException as exc:
            summary.update(status='failed', cleanup_error=str(exc))
        summary['elapsed_seconds'] = time.monotonic() - started
        save_json(directory / 'summary.json', summary)
    if summary['status'] != 'passed': raise RuntimeError(summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify-only', type=Path, help='recheck saved evidence without a broker')
    parser.add_argument('--artifacts', type=Path, help='fresh evidence directory (retained even on success)')
    parser.add_argument('--single-node', action='store_true')
    parser.add_argument('--transactions', action='store_true', help='single-node transaction/crash mode')
    parser.add_argument('--no-faults', action='store_true', help='smoke workload or live HW mutation proof')
    parser.add_argument('--duration', type=float, default=1800)
    parser.add_argument('--seed', type=int, default=0)
    parser.add_argument('--port-base', type=int, default=9092)
    parser.add_argument('--broker-bin', type=Path)
    args = parser.parse_args()
    if args.verify_only:
        print(json.dumps(verify_artifacts(args.verify_only), sort_keys=True))
        return
    if args.duration <= 0: parser.error('--duration must be positive')
    if args.transactions and not args.single_node:
        parser.error('transaction mode requires --single-node until M10')
    run_live(args)


if __name__ == '__main__':
    main()
