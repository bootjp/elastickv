"""Summarize complete, matched VM runs and check the reported counts."""
import csv
import re
from pathlib import Path
from statistics import median

root = Path(__file__).resolve().parent
rows = []
for pair in range(1, 4):
    for version in ('one', 'two'):
        stem = f'pair-{pair}-{version}'
        assert (root / f'{stem}.exit').read_text().strip() == '0', stem
        log = (root / f'{stem}.log').read_text()
        assert re.search(r'^--- PASS: TestPerformanceReadWriteSplit', log, re.M), stem
        assert 'source_leader=0 target_leader=1' in log, stem
        bounds = re.findall(r'preflight-duration lower_ms=([\d.]+) upper_ms=([\d.]+)', log)
        assert len(bounds) == 1, stem
        lower, upper = (float(v) / 1000 for v in bounds[0])
        assert 0 < lower < upper
        row = dict(pair=pair, version=version, lower_s=round(lower, 6), upper_s=round(upper, 6),
                   midpoint_s=round((lower + upper) / 2, 7))
        for write, op in (('false', 'read'), ('true', 'write')):
            pattern = (rf'phase=migration-preflight write={write} elapsed_s=([\d.]+) '
                       r'success=(\d+) errors=(\d+) ops_s=([\d.]+) p50_ms=[\d.]+ p99_ms=([\d.]+)')
            result = re.findall(pattern, log)
            assert len(result) == 1, (stem, op)
            elapsed, success, errors, rate, p99 = result[0]
            row['attributed_s'] = float(elapsed)
            row[f'{op}_success'] = int(success)
            row[f'{op}_errors'] = int(errors)
            row[f'{op}_ops_s'] = float(rate)
            row[f'{op}_p99_ms'] = float(p99)
        recovery = re.findall(r'recovery path=(\S+) planned=600 sent=600 success=600 errors=0 ', log)
        assert len(set(recovery)) == 5, stem
        rows.append(row)

with (root / 'metrics.csv').open('w', newline='') as out:
    writer = csv.DictWriter(out, fieldnames=rows[0].keys(), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)

for name in ('one', 'two'):
    runs = [r for r in rows if r['version'] == name]
    print(name, 'upper <= 5s:', sum(r['upper_s'] <= 5 for r in runs), '/', len(runs))
    for key in rows[0]:
        if key not in ('version','pair'):
            aggregate = sum if key.endswith(('_errors', '_success')) else median
            print(f'  {key}: {aggregate(r[key] for r in runs):.4f}')
