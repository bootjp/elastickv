"""Check completeness and provenance of the six-run comparison."""
from pathlib import Path
import hashlib
import json
import re

root = Path(__file__).parent
metadata = json.loads((root / 'metadata.json').read_text())
summary = json.loads((root / 'summary.json').read_text())
assert len(summary['runs']) == 6
assert {(r['pair'], r['variant']) for r in summary['runs']} == {(p, v) for p in range(1, 4) for v in ['baseline', 'candidate']}
for run in summary['runs']:
    label = f"pair-{run['pair']}-{run['variant']}"
    text = (root / f'{label}.log').read_text()
    assert int((root / f'{label}.exit').read_text()) == 0
    assert '--- PASS: TestPerformanceReadWriteSplit' in text
    assert 'source_leader=0 target_leader=1 fsm_sync_mode=""' in text
    assert len(re.findall(r'planned=600 sent=600 success=600 errors=0', text)) == 5
    assert re.search(r'PERF phase=migration-preflight write=false .*?errors=0 ', text)
    assert re.search(r'PERF phase=migration-preflight write=true .*?errors=0 ', text)
    for line in text.splitlines():
        if re.search(r'PERF phase=(before-|after-|same-group-)', line):
            assert re.search(r' errors=0 ', line), line
for name, checksum in metadata['binaries'].items():
    assert f'{checksum}  {name}' in (root / 'environment.txt').read_text()
assert (root / 'vm-final-state.log').read_text().strip() == 'status: stopped'
manifest = json.loads((root / 'manifest.sha256.json').read_text())
assert all(hashlib.sha256((root / name).read_bytes()).hexdigest() == checksum for name, checksum in manifest.items())
print('Verified 6 successful runs, 18,000 unique write/readbacks, matching binary hashes, final VM stopped, and artifact manifest.')
