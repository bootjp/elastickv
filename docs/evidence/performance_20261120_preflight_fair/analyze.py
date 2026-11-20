"""Summarize separate prefix and full studies without pooling their metrics."""
from pathlib import Path
import csv
import json
import re
import statistics
import sys

root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
records = []
runs = []
for mode, pair in [('full', p) for p in range(1, 4)]:
    for variant in ['baseline', 'candidate']:
        label = f'{mode}-{pair}-{variant}'
        log = root / f'{label}.log'
        assert log.exists(), label
        content = log.read_text()
        code = int((root / f'{label}.exit').read_text())
        match = re.search(r'PERF cross-group done_ms=(\d+)', content)
        recoveries = re.findall(r'PERF recovery path=(\S+) planned=600 sent=(\d+) success=(\d+) errors=(\d+)', content)
        assert code == 0, label
        assert 'PERF native-partition-runtime source=true target=true' in content, label
        if mode == 'full':
            assert match and len(recoveries) == 5, label
            assert all(a == '600' and b == '600' and c == '0' for _, a, b, c in recoveries), label
        else:
            assert not match and not recoveries, label
            assert 'PERF preflight-pilot stopped_after_witness=true' in content, label
        bounds = re.search(r'PERF preflight-duration lower_ms=([\d.]+) upper_ms=([\d.]+)', content)
        assert bounds, label
        work = re.search(r'PERF preflight-work families=(\d+) completed=(\d+) versions=(\d+)', content)
        assert work and work[1] == work[2], label
        elapsed = int(match[1]) / 1000 if mode == 'full' else None
        runs.append({'mode': mode, 'pair': pair, 'variant': variant, 'split_s': elapsed, 'exit': code, 'recovery_success': 3000 if mode == 'full' else 0, 'preflight_versions': int(work[3]), 'preflight_families': int(work[1]), 'preflight_lower_ms': float(bounds[1]), 'preflight_upper_ms': float(bounds[2]), 'recovery_max_lag_ms': max((int(v) for v in re.findall(r'PERF recovery path=.*?max_lag_ms=(\d+)', content)), default=None)})
        pattern = r'PERF phase=(\S+) write=(true|false) elapsed_s=([\d.]+) success=(\d+) errors=(\d+) ops_s=([\d.]+) p50_ms=([\d.]+) p99_ms=([\d.]+)'
        for m in re.finditer(pattern, content):
            phase, write, secs, success, errors, qps, p50, p99 = m.groups()
            records.append({'mode': mode, 'pair': pair, 'variant': variant, 'phase': phase, 'operation': 'write' if write == 'true' else 'read', 'elapsed_s': float(secs), 'success': int(success), 'errors': int(errors), 'ops_s': float(qps), 'p50_ms': float(p50), 'p99_ms': float(p99)})

assert len(runs) == 6
assert {(r['mode'], r['pair'], r['variant']) for r in runs} == {(mode, p, v) for mode, p in [('full', p) for p in range(1, 4)] for v in ['baseline', 'candidate']}

with (root / 'metrics.csv').open('w') as f:
    writer = csv.DictWriter(f, fieldnames=list(records[0]))
    writer.writeheader()
    writer.writerows(records)
summary = {'runs': runs, 'studies': {}}
for mode in ['full']:
    medians = {}
    for variant in ['baseline', 'candidate']:
        items = [r for r in runs if r['mode'] == mode and r['variant'] == variant]
        medians[variant] = {'n': len(items), 'preflight_upper_ms': statistics.median(r['preflight_upper_ms'] for r in items), 'all_preflight_under_5s': all(r['preflight_upper_ms'] <= 5000 for r in items), 'metrics': {}}
        if mode == 'full':
            medians[variant]['split_s'] = statistics.median(r['split_s'] for r in items)
        for phase, operation in sorted({(r['phase'], r['operation']) for r in records}):
            selected = [r for r in records if r['mode'] == mode and r['variant'] == variant and r['phase'] == phase and r['operation'] == operation]
            if selected:
                medians[variant]['metrics'][f'{phase}/{operation}'] = {field: statistics.median(r[field] for r in selected) for field in ['elapsed_s', 'success', 'errors', 'ops_s', 'p50_ms', 'p99_ms']}
    summary['studies'][mode] = medians
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
