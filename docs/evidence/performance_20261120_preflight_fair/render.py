"""Render separate prefix/full diagnostics without pooling unequal studies."""
from pathlib import Path
import csv
import json
import statistics

root = Path(__file__).parent
s = json.loads((root / 'summary.json').read_text())
with (root / 'metrics.csv').open() as f:
    rows = list(csv.DictReader(f))
lines = ['# 同条件のpreflightとsplit比較', '', '比較元は16ページ・1秒tick、候補は最大8ページ・512 KiB・200 ms予算・進捗後100 msの設定。両版ともP0修正を含む。', '', '最終候補でDoneと移動後の検証まで進める3組（AB/BA/AB）を集計する。旧版互換性の追修前に測ったprefixはtrials/merged-prefixへ分けた。', '', '| モード / 組 / 版 | preflight下限〜上限 (s) | 検査version数 | Done (s) | 回復検証の最大lag (ms) |', '| --- | ---: | ---: | ---: | ---: |']
for run in s['runs']:
    done = f"{run['split_s']:.3f}" if run['split_s'] is not None else '未測定'
    lines.append(f"| {run['mode']} / {run['pair']} / {run['variant']} | {run['preflight_lower_ms']/1000:.3f}〜{run['preflight_upper_ms']/1000:.3f} | {run['preflight_versions']:,} | {done} | {run['recovery_max_lag_ms']} |")
for mode in ['full']:
    lines.extend(['', f'## {mode}の性能', '', '| 区間 / 指標 | baseline | candidate | 変化 |', '| --- | ---: | ---: | ---: | ---: |'])
    for phase in ['before-mixed', 'before-read', 'before-write', 'migration-preflight', 'after-mixed', 'after-read', 'after-write']:
        for operation in ['read', 'write']:
            for field in ['ops_s', 'p99_ms']:
                values = {}
                for variant in ['baseline', 'candidate']:
                    selected = [float(r[field]) for r in rows if r['mode'] == mode and r['variant'] == variant and r['phase'] == phase and r['operation'] == operation]
                    if selected:
                        values[variant] = selected
                if len(values) != 2:
                    continue
                cells = [f'{statistics.median(values[v]):.3f} ({min(values[v]):.3f}〜{max(values[v]):.3f})' for v in ['baseline', 'candidate']]
                a, b = (statistics.median(values[v]) for v in ['baseline', 'candidate'])
                delta = f'{100*(b/a-1):+.1f}%' if a else '—'
                lines.append('| ' + f'{phase} / {operation} {field}' + ' | ' + ' | '.join(cells) + f' | {delta} |')
    lines.extend(['', '| 区間 / 操作 | baseline 成功 / エラー | candidate 成功 / エラー |', '| --- | ---: | ---: |'])
    for phase in ['migration-preflight', 'cross-group-during', 'after-mixed', 'after-read', 'after-write']:
        for operation in ['read', 'write']:
            cells = []
            for variant in ['baseline', 'candidate']:
                selected = [r for r in rows if r['mode'] == mode and r['variant'] == variant and r['phase'] == phase and r['operation'] == operation]
                cells.append(f"{sum(int(r['success']) for r in selected):,} / {sum(int(r['errors']) for r in selected):,}")
            lines.append('| ' + phase + '/' + operation + ' | ' + ' | '.join(cells) + ' |')
lines.extend(['', '括弧は各試行の最小〜最大。代表値は各版3回の中央値。p99は成功RPCの試行ごとのp99であり、全サンプルを混ぜたp99ではない。', '5秒判定には10 ms周期の観測上限を使う。負荷区間の集計は従来の100 ms観測のまま。phaseの長さが異なるため総成功件数も変わる。', 'closed-loop診断であり、予定到着からの遅延・5組以上・障害注入を含む前景性能の正式採用ゲートは未評価。', 'fullは各版とも5経路×600件の移動後書込みと正規所有者からの固有値の読み返しを確認する。prefixでは移動完了も回復も検証しない。'])
(root / 'results.md').write_text('\n'.join(lines) + '\n')
