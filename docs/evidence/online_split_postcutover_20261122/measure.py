import json,os,gzip,shutil,time,subprocess,hashlib
from pathlib import Path
work=Path(__file__).resolve().parent
os.chdir(work)
files=json.loads((work/'expected.json').read_text())
actual={k:hashlib.file_digest(open(k,'rb'),'sha256').hexdigest() for k in files}
assert actual==files,(actual,files)
(work/'verified.json').write_text(json.dumps(actual))
for name, binary in [('baseline','baseline-server.test'),('candidate','candidate-server.test')]:
    if name=='candidate':
        limit=time.monotonic()+900
        while not (work/'candidate-ready.json').exists():
            if time.monotonic()>limit:raise RuntimeError('candidate not released')
            time.sleep(1)
        release=json.loads((work/'candidate-ready.json').read_text())
        assert hashlib.file_digest(open(binary,'rb'),'sha256').hexdigest()==release['sha256']
    out=work/name
    out.mkdir()
    env={k:v for k,v in os.environ.items() if not k.startswith('ELASTICKV_')}
    env.update(GOMAXPROCS='8', ELASTICKV_LOAD_BINARY=str(work/'loadtest-window'),ELASTICKV_LOAD_RAMP='1',ELASTICKV_PERF_PREFLIGHT_TIMING='1',ELASTICKV_LOAD_RATES='100,300,500,1000',ELASTICKV_LOAD_STAGE_DURATION='90s',ELASTICKV_LOAD_CONCURRENCY='32',ELASTICKV_LOAD_READ_PERCENT='50',ELASTICKV_LOAD_SPLIT_STAGE='1',ELASTICKV_LOAD_SPLIT='cross',ELASTICKV_RPC_TRACE=str(out/'rpc.csv'))
    if name=='candidate':env['ELASTICKV_LOAD_REQUIRE_ONLINE']='1'
    with (out/'run.log').open('xb') as raw,(out/'markers.jsonl').open('x',buffering=1) as timeline:
        p=subprocess.Popen([str(work/binary),'-test.run=^TestLinuxSplitLoadRamp$','-test.count=1','-test.v','-test.timeout=15m'],env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        for line in p.stdout:
            ns=time.time_ns()
            raw.write(line)
            if b'PERF migration elapsed_ms' in line or b'LOAD ' in line or b'PERF preflight' in line or b'PERF recovery path' in line or b'--- PASS:' in line or b'--- FAIL:' in line:
                print(json.dumps([ns,line.decode(errors='replace').strip()]),file=timeline)
                raw.flush()
        rc=p.wait()
        (out/'exit.txt').write_text(str(rc))
    if (out/'rpc.csv').exists():
        with (out/'rpc.csv').open('rb') as src,gzip.open(out/'rpc.csv.gz','wb',compresslevel=3) as dest:shutil.copyfileobj(src,dest)
    print(name,rc,flush=True)
    if rc:raise RuntimeError(f"{name} fixture failed: {rc}")
(work/'complete').write_text('done\n')
