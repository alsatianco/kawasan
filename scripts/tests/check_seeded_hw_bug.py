#!/usr/bin/env python3
"""Build a disposable broker with an HW bug; require the live checker to catch I1.

Copies only build inputs. The main worktree, branch, user changes and normal
build directory remain intact. A compiler/client/startup failure is NOT proof.
"""
import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

from consistency_checker import save_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifacts', type=Path, required=True, help='new proof directory')
    parser.add_argument('--jobs', type=int, default=4)
    parser.add_argument('--port-base', type=int, default=20092)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    directory = args.artifacts.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    source = directory / 'source'
    source.mkdir()
    for name in ['CMakeLists.txt', 'cmake', 'include', 'src', 'tools', 'config', 'vcpkg.json']:
        path = root / name
        if path.is_dir():
            shutil.copytree(path, source / name)
        else:
            shutil.copy2(path, source / name)
    broker_source = source / 'src/broker/kawasan_broker.cpp'
    text = broker_source.read_text()
    needle = 'const Offset high_watermark = hw_opt.value_or(log->highWatermark());'
    replacement = ('const Offset high_watermark = std::max<Offset>(0, '
                   'hw_opt.value_or(log->highWatermark()) - 1);')
    if text.count(needle) != 1:
        raise RuntimeError('HW mutation point changed; review the proof before updating it')
    broker_source.write_text(text.replace(needle, replacement))
    save_json(directory / 'mutation.json', {'file': 'src/broker/kawasan_broker.cpp',
                                          'before': needle, 'after': replacement})
    build = directory / 'build'
    with (directory / 'build.log').open('w') as stream:
        subprocess.run(['cmake', '-S', str(source), '-B', str(build),
                        '-DCMAKE_BUILD_TYPE=RelWithDebInfo', '-DKAWASAN_BUILD_TESTS=OFF',
                        '-DKAWASAN_BUILD_EXAMPLES=OFF'], check=True, stdout=stream,
                       stderr=subprocess.STDOUT, timeout=120)
        subprocess.run(['cmake', '--build', str(build), '-j', str(args.jobs),
                        '--target', 'kawasan-broker'], check=True, stdout=stream,
                       stderr=subprocess.STDOUT, timeout=900)
    with (directory / 'checker.log').open('w') as stream:
        result = subprocess.run([sys.executable, str(root / 'scripts/tests/consistency_checker.py'),
                                 '--single-node', '--no-faults', '--duration', '3',
                                 '--port-base', str(args.port_base), '--broker-bin',
                                 str(build / 'tools/kawasan-broker'), '--artifacts',
                                 str(directory / 'rejected')], stdout=stream,
                                stderr=subprocess.STDOUT, timeout=120)
    summary = json.loads((directory / 'rejected/summary.json').read_text())
    if result.returncode == 0 or not summary.get('error', '').startswith('Violation: I1:'):
        raise RuntimeError(f'checker did not reject the seeded HW bug with I1: {summary}')
    save_json(directory / 'proof.json', {'status': 'passed', 'invariant': 'I1',
                                       'mutation': replacement, 'error': summary['error']})
    print(f'PASS: live seeded HW bug rejected by I1; evidence: {directory}', flush=True)


if __name__ == '__main__':
    main()
