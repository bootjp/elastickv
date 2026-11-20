from pathlib import Path
import csv,json,re,statistics
r=Path(__file__).parent
rows=[]
for pair in range(1,4):
 for variant in ['baseline','candidate']:
  label=f'prefix-{pair}-{variant}';s=(r/(label+'.log')).read_text();assert int((r/(label+'.exit')).read_text())==0
  assert 'stopped_after_witness=true' in s and 'PERF cross-group done_ms=' not in s
  bounds=re.search(r'preflight-duration lower_ms=([\d.]+) upper_ms=([\d.]+)',s);assert bounds
  row={'pair':pair,'variant':variant,'lower_ms':float(bounds[1]),'upper_ms':float(bounds[2])}
  for op,flag in [('read','false'),('write','true')]:
   m=re.search(r'phase=migration-preflight write='+flag+r' elapsed_s=([\d.]+) success=(\d+) errors=(\d+) ops_s=([\d.]+) p50_ms=([\d.]+) p99_ms=([\d.]+)',s);assert m
   row.update({op+'_success':int(m[2]),op+'_errors':int(m[3]),op+'_ops_s':float(m[4]),op+'_p99_ms':float(m[6])})
  rows.append(row)
with (r/'metrics.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
med={v:{k:statistics.median(row[k] for row in rows if row['variant']==v) for k in ['upper_ms','read_ops_s','write_ops_s','read_p99_ms','write_p99_ms']} for v in ['baseline','candidate']}
(r/'summary.json').write_text(json.dumps({'scope':'exploratory, before final legacy corrections; not full migration','runs':rows,'medians':med},indent=2)+'\n')
lines=['# 旧版互換性追修前のprefix比較','','最終候補の統計には含めない。全試行はpreflight完了witnessで停止し、Done・移動後回復は未検証。','','| 指標の中央値 | baseline | candidate |','| --- | ---: | ---: |']
for k in med['baseline']:lines.append(f"| {k} | {med['baseline'][k]:.3f} | {med['candidate'][k]:.3f} |")
lines+=['','candidateは3/3回で観測上限5秒以内。read/write p99の中央値はどちらも比較元より大きく、初回だけの改善から前景性能の改善は主張できない。','前景preflightエラーは全6試行でread/writeとも0件。負荷はclosed-loopで、版ごとに測定時間が異なる。','入力とSHAはmetadata.jsonとmeasured_sources.tar.gz、環境はprefix-environment.txt、全試行の値はmetrics.csvに保存する。']
(r/'README.md').write_text('\n'.join(lines)+'\n')
