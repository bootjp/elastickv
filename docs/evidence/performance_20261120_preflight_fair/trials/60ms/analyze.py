"""Summarize paired full migration diagnostics without dropping errors."""
from pathlib import Path
import csv
import json
import re
import statistics
import sys

root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
records = []
runs = []
for pair in range(1, 4):
    for variant in ['baseline', 'candidate']:
        label = f'final-{pair}-{variant}'
        log = root / f'{label}.log'
        if not log.exists():
            continue
        content = log.read_text()
        code = int((root / f'{label}.exit').read_text())
        match = re.search(r'PERF cross-group done_ms=(\d+)', content)
        recoveries = re.findall(r'PERF recovery path=(\S+) planned=600 sent=(\d+) success=(\d+) errors=(\d+)', content)
        assert code == 0 and match and len(recoveries) == 5, label
        assert all(a == '600' and b == '600' and c == '0' for _, a, b, c in recoveries), label
        bounds = re.search(r'PERF preflight-duration lower_ms=([\d.]+) upper_ms=([\d.]+)', content)
        assert bounds, label
        elapsed = int(match[1]) / 1000
        runs.append({'pair': pair, 'variant': variant, 'split_s': elapsed, 'exit': code, 'recovery_success': 3000, 'preflight_lower_ms': float(bounds[1]), 'preflight_upper_ms': float(bounds[2]), 'recovery_max_lag_ms': max(int(v) for v in re.findall(r'PERF recovery path=.*?max_lag_ms=(\d+)', content))})
        pattern = r'PERF phase=(\S+) write=(true|false) elapsed_s=([\d.]+) success=(\d+) errors=(\d+) ops_s=([\d.]+) p50_ms=([\d.]+) p99_ms=([\d.]+)'
        for m in re.finditer(pattern, content):
            phase, write, secs, success, errors, qps, p50, p99 = m.groups()
            records.append({'pair': pair, 'variant': variant, 'phase': phase, 'operation': 'write' if write == 'true' else 'read', 'elapsed_s': float(secs), 'success': int(success), 'errors': int(errors), 'ops_s': float(qps), 'p50_ms': float(p50), 'p99_ms': float(p99)})

with (root / 'metrics.csv').open('w') as f:
    writer = csv.DictWriter(f, fieldnames=list(records[0]))
    writer.writeheader()
    writer.writerows(records)
summary = {'runs': runs, 'medians': {}}
for variant in ['baseline', 'candidate']:
    variant_runs = [r for r in runs if r['variant'] == variant]
    if not variant_runs:
        continue
    summary['medians'][variant] = {'split_s': statistics.median(r['split_s'] for r in variant_runs), 'metrics': {}}
    for phase, operation in sorted({(r['phase'], r['operation']) for r in records}):
        items = [r for r in records if r['variant'] == variant and r['phase'] == phase and r['operation'] == operation]
        if not items:
            continue
        summary['medians'][variant]['metrics'][f'{phase}/{operation}'] = {
            field: statistics.median(r[field] for r in items)
            for field in ['elapsed_s', 'success', 'errors', 'ops_s', 'p50_ms', 'p99_ms']
        }
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
