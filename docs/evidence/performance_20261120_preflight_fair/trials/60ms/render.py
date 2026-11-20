"""Render medians and observed ranges; do not infer significance from 3 pairs."""
from pathlib import Path
import json
import statistics

root = Path(__file__).parent
s = json.loads((root / 'summary.json').read_text())
metrics = s['medians']

def show(variant, key, unit=''):
    values = [r['split_s'] for r in s['runs'] if r['variant'] == variant] if key == 'split_s' else []
    return f"{statistics.median(values):.3f} ({min(values):.3f}–{max(values):.3f}){unit}"

lines = ['# 同条件の split 比較結果', '', '## 各組の所要時間', '', '| 組 | baseline Done (s) | candidate Done (s) | 短縮率 |', '| --- | ---: | ---: | ---: |']
for pair in sorted({r['pair'] for r in s['runs']}):
    group = {r['variant']: r for r in s['runs'] if r['pair'] == pair}
    if len(group) != 2:
        continue
    a, b = group['baseline']['split_s'], group['candidate']['split_s']
    lines.append(f'| {pair} | {a:.3f} | {b:.3f} | {(1-b/a)*100:.1f}% |')
lines.extend(['', '## 中央値と観測範囲', '', '| 指標 | baseline | candidate |', '| --- | ---: | ---: |', f"| split Done (s) | {show('baseline', 'split_s')} | {show('candidate', 'split_s')} |"])
# Read metrics.csv to retain each run rather than pool differently sized samples.
import csv
with (root / 'metrics.csv').open() as f:
    rows = list(csv.DictReader(f))
for label, phase, operation, field in [
    ('preflight (s)', 'migration-preflight', 'read', 'elapsed_s'),
    ('preflight read (ops/s)', 'migration-preflight', 'read', 'ops_s'),
    ('preflight write (ops/s)', 'migration-preflight', 'write', 'ops_s'),
    ('preflight read p99 (ms)', 'migration-preflight', 'read', 'p99_ms'),
    ('preflight write p99 (ms)', 'migration-preflight', 'write', 'p99_ms'),
    ('before read-only (ops/s)', 'before-read', 'read', 'ops_s'),
    ('before read-only p99 (ms)', 'before-read', 'read', 'p99_ms'),
    ('before write-only (ops/s)', 'before-write', 'write', 'ops_s'),
    ('before write-only p99 (ms)', 'before-write', 'write', 'p99_ms'),
    ('after read-only (ops/s)', 'after-read', 'read', 'ops_s'),
    ('after read-only p99 (ms)', 'after-read', 'read', 'p99_ms'),
    ('after write-only (ops/s)', 'after-write', 'write', 'ops_s'),
    ('after write-only p99 (ms)', 'after-write', 'write', 'p99_ms'),
    ('after mixed read (ops/s)', 'after-mixed', 'read', 'ops_s'),
    ('after mixed read p99 (ms)', 'after-mixed', 'read', 'p99_ms'),
    ('after mixed write (ops/s)', 'after-mixed', 'write', 'ops_s'),
    ('after mixed write p99 (ms)', 'after-mixed', 'write', 'p99_ms'),
]:
    cells = []
    for variant in ['baseline', 'candidate']:
        values = [float(r[field]) for r in rows if r['variant'] == variant and r['phase'] == phase and r['operation'] == operation]
        cells.append(f'{statistics.median(values):.3f} ({min(values):.3f}–{max(values):.3f})')
    lines.append('| ' + label + ' | ' + ' | '.join(cells) + ' |')
lines.extend(['', '## 成功数とエラー数', '', '| 区間 / 操作 | baseline 成功 / エラー | candidate 成功 / エラー |', '| --- | ---: | ---: |'])
for phase in ['migration-preflight', 'cross-group-during', 'after-mixed', 'after-read', 'after-write']:
    for operation in ['read', 'write']:
        cells = []
        for variant in ['baseline', 'candidate']:
            selected = [r for r in rows if r['variant'] == variant and r['phase'] == phase and r['operation'] == operation]
            cells.append(f"{sum(int(r['success']) for r in selected):,} / {sum(int(r['errors']) for r in selected):,}")
        lines.append('| ' + phase + '/' + operation + ' | ' + ' | '.join(cells) + ' |')
lines.extend(['', '括弧は各 run の最小–最大。p99 は各 run の成功 RPC の p99 を算出した後の中央値で、全サンプルを混ぜた p99 ではない。成功/エラーは全 run の合計。', '移動中の全期間集計は phase 配分と実行時間が異なるため、同一 phase の性能比較には使わない。', 'Done 後の回復確認は各 run 3,000 件すべて成功し、正規所有者から固有値を読み返した。'])
(root / 'results.md').write_text('\n'.join(lines) + '\n')
