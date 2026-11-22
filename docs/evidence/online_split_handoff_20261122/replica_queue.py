import hashlib,json,os,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parent
previous=root.parent/'final'
limit=time.monotonic()+1200
while True:
    try: state=json.loads((previous/'isolation-final/host-isolation.json').read_text())
    except (FileNotFoundError,json.JSONDecodeError):state={}
    if 'restored' in state:
        assert state.get('exit_code')==0 and not state['restore_errors'],state
        break
    if time.monotonic()>limit:raise RuntimeError('previous comparison did not finish')
    time.sleep(2)
for name in ['baseline-server.test','candidate-server.test','loadtest-window']:
    os.link(previous/name,root/name)
files=['baseline-server.test','candidate-server.test','loadtest-window','measure.py','protocol.json','isolate-load-host.py','queue.py','candidate-ready.json']
expected={n:hashlib.file_digest(open(root/n,'rb'),'sha256').hexdigest() for n in files}
assert expected['baseline-server.test']=='bd98c2ca5c46ce35d9aad99d4e34acc76be9c5d6f467a2542b6e7c85dca78f82'
assert expected['candidate-server.test']==json.loads((root/'candidate-ready.json').read_text())['sha256']
assert expected['loadtest-window']=='8350c069ee061ac2c89a1c876b3391a977d86eb724eca25837625aba993f8acc'
(root/'expected.json').write_text(json.dumps(expected,indent=2)+'\n')
for attempt in range(3):
    quiet=0
    while quiet<3:
        states=[subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip() for unit in ['fwupd.service','fwupd-refresh.service']]
        quiet=quiet+1 if all(x in ['inactive','failed'] for x in states) else 0
        if time.monotonic()>limit:raise RuntimeError('firmware daemon did not become idle')
        time.sleep(5)
    folder=root/('isolation-'+str(attempt+1))
    rc=subprocess.call(['python3',str(root/'isolate-load-host.py'),str(folder),'--','python3',str(root/'measure.py')])
    state=json.loads((folder/'host-isolation.json').read_text())
    if rc==0:
        (root/'queue-complete').write_text('done\n')
        break
    if 'started_ns' in state:raise RuntimeError('replication failed after isolation entered; do not retry results')
else:raise RuntimeError('isolation admission repeatedly failed')
