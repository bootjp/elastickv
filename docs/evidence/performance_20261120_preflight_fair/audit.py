"""Verify final paired full migrations and their measured-source provenance."""
from pathlib import Path
import hashlib
import json
import re
import sys
import tarfile

root = Path(__file__).resolve().parent
metadata = json.loads((root / 'metadata.json').read_text())
summary = json.loads((root / 'summary.json').read_text())
assert len(summary['runs']) == 6
assert len([r for r in summary['runs'] if r['mode'] == 'prefix']) == 0
assert len([r for r in summary['runs'] if r['mode'] == 'full']) == 6
with tarfile.open(root / 'measured_sources.tar.gz') as archive:
    for group in ['candidate', 'common', 'baseline']:
        for name, expected in metadata[group + '_sources'].items():
            member = archive.extractfile(group + '/' + name)
            assert member and hashlib.sha256(member.read()).hexdigest() == expected, (group, name)
with tarfile.open(root / 'test_sources.tar.gz') as archive:
    for name, expected in metadata['test_sources'].items():
        member = archive.extractfile(name)
        assert member and hashlib.sha256(member.read()).hexdigest() == expected, name
repo = root.parents[2]
workspace_mismatches = [name for name, expected in metadata['candidate_sources'].items() if hashlib.sha256((repo / name).read_bytes()).hexdigest() != expected]
if '--measured-only' not in sys.argv:
    assert not workspace_mismatches, workspace_mismatches
hashes = dict((name, digest) for digest, name in re.findall(r'^([a-f0-9]{64})\s+(baseline\.test|candidate\.test)$', (root / 'final-environment.txt').read_text(), re.M))
assert hashes == metadata['binaries']
running = re.search(r'^([a-f0-9]{64})\s+/proc/\d+/exe$', (root / 'full-1-running-binary.txt').read_text(), re.M)
assert running and running[1] == metadata['binaries']['candidate.test']
assert all(run['exit'] == 0 and run['recovery_success'] == (3000 if run['mode'] == 'full' else 0) for run in summary['runs'])
deltas = {}
for mode, medians in summary['studies'].items():
    deltas[mode] = {}
    for operation in ['read', 'write']:
        key = 'migration-preflight/' + operation
        deltas[mode][operation] = {field: 100 * (medians['candidate']['metrics'][key][field] / medians['baseline']['metrics'][key][field] - 1) for field in ['p99_ms', 'ops_s']}
result = {'source_and_binary_provenance': 'verified', 'workspace_matches_measured_sources': not workspace_mismatches, 'workspace_source_mismatches': workspace_mismatches, 'prefix_runs': 0, 'full_runs': 6, 'recovery_success': 18000,
          'candidate_all_preflight_upper_under_5s': all(run['preflight_upper_ms'] <= 5000 for run in summary['runs'] if run['variant'] == 'candidate'),
          'foreground_delta_pct': deltas, 'formal_adoption_gate_evaluated': False,
          'limitation': 'three closed-loop full pairs; no planned-arrival latency or failure-injection campaign'}
(root / 'audit.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
