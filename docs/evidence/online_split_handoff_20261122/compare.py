import csv,gzip,json,sys,re
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path
root=Path(__file__).resolve().parent

def parse_timestamp(s):
    sec,frac=s.rstrip('Z').split('.')
    return int(datetime.fromisoformat(sec).replace(tzinfo=timezone.utc).timestamp())*10**9+int(frac.ljust(9,'0'))

def percentile(samples):
    return sorted(samples)[(len(samples)-1)*99//100]/1e6 if samples else None

def stats(rows,lo,hi):
    selected=[r for r in rows if lo<=r[1]<hi]
    dur=(hi-lo)/1e9
    out={'seconds':dur}
    for kind in ['read','write']:
        ops=[r for r in selected if r[3]==kind]
        good=[r[2] for r in ops if r[4]=='OK']
        err=Counter(r[4] for r in ops if r[4]!='OK')
        out[kind]={'attempts':len(ops),'success':len(good),'ops_s':len(good)/dur,'success_p99_ms':percentile(good),'all_p99_ms':percentile([r[2] for r in ops]),'errors':dict(err)}
    return out

def load(name):
    d=root/name
    assert (d/'exit.txt').read_text().strip()=='0',name
    markers=[json.loads(l) for l in (d/'markers.jsonl').read_text().splitlines()]
    events=[json.loads(s.split('LOAD ',1)[1]) for t,s in markers if 'LOAD {' in s]
    cfg=next(e['config'] for e in events if e['type']=='configuration')
    assert cfg['rates']==[100,300,500,1000] and cfg['stage_duration_ns']==90000000000 and cfg['concurrency']==32 and cfg['read_percent']==50
    stages=[e['stage'] for e in events if e['type']=='stage']
    assert [s['offered_rps'] for s in stages]==cfg['rates']
    audit=next(e for e in events if e['type']=='audit')
    with gzip.open(d/'rpc.csv.gz','rt') as f:
        rows=[(int(i),int(t),int(dt),kind,code) for i,t,dt,kind,code in csv.reader(f)]
    assert len(rows)==len({r[0] for r in rows})
    offset=0
    for s in stages:
        part=[r for r in rows if offset<=r[0]<offset+s['planned']]
        for kind in ['read','write']:
            ops=[r for r in part if r[3]==kind]
            good=[r for r in ops if r[4]=='OK']
            m=s[kind]
            assert len(ops)==m['attempts'] and len(good)==m['success'],(name,kind,s['index'])
            assert Counter(r[4] for r in ops if r[4]!='OK')==(m['errors'] or {})
            assert percentile([r[2] for r in good])==m['successful_latency']['p99_ms']
        assert len(part)+s['dropped_capacity']+s['dropped_scheduler']==s['offered']==s['planned']
        offset+=s['planned']
    assert all(0<=r[0]<offset for r in rows)
    assert len(rows)==sum(s['read']['attempts']+s['write']['attempts'] for s in stages)
    assert audit['complete'] and audit['acknowledged_writes']>0
    assert sum(s['write']['success'] for s in stages)==audit['acknowledged_writes'] and audit['verified']
    assert audit['acknowledged_writes']*3==audit['expected_observations']==audit['audit']['success']==audit['audit']['attempts']
    phase=[]
    recovery={}
    timing={}
    for ns,s in markers:
        if m:=re.search(r'PERF migration elapsed_ms=(\d+) phase=(\d+)',s):phase.append((ns,int(m[1]),int(m[2])))
        if m:=re.search(r'PERF recovery path=(\S+) planned=(\d+) sent=(\d+) success=(\d+) errors=(\d+)',s):recovery[m[1]]=list(map(int,m.groups()[1:]))
        if m:=re.search(r'preflight-duration lower_ms=([\d.]+) upper_ms=([\d.]+)',s):timing.update(lower_ms=float(m[1]),upper_ms=float(m[2]))
        if m:=re.search(r'preflight-work families=(\d+) completed=(\d+) versions=(\d+)',s):timing.update(families=int(m[1]),completed=int(m[2]),versions=int(m[3]))
        if m:=re.search(r'online_capture_version=(\d+) sequence=(\d+) seal_sequence=(\d+)',s):timing.update(capture_version=int(m[1]),sequence=int(m[2]),seal_sequence=int(m[3]))
    assert set(recovery)=={'outside-direct','retained-direct','retained-forwarded','moved-direct','moved-forwarded'}
    assert phase[-1][2]==7 and len(recovery)==5 and all(a==[600,600,600,0] for a in recovery.values())
    anchors=[ns-ms*10**6 for ns,ms,_ in phase]
    end,elapsed,_=phase[-1]
    start=end-elapsed*10**6
    last300=parse_timestamp(stages[1]['started'])+cfg['stage_duration_ns']
    assert parse_timestamp(stages[1]['started'])<start<last300
    assert next(ns for ns,_,phase_id in phase if phase_id==5) < last300-10*10**9, 'tail must be after cutover'
    totals={}
    for kind in ['read','write']:
        err=Counter()
        for s in stages:err.update(s[kind]['errors'] or {})
        totals[kind]={'success':sum(s[kind]['success'] for s in stages),'errors':dict(err)}
    assert timing['capture_version']==1 and timing['sequence']==timing['seal_sequence']>0
    phases=[dict(phase=ph,observed_elapsed_ms=ms,metrics=stats(rows,ns,phase[i+1][0])) for i,(ns,ms,ph) in enumerate(phase[:-1])]
    return rows,start,end,min(end,last300)-start,{'phases':phases,'config':cfg,'stages':stages,'stage_300_end_ns':last300,'after_cutover_300_tail':stats(rows,last300-10*10**9,last300),'tail_includes_cleanup':end>last300-10*10**9,'split_all':stats(rows,start,end),'full_run':totals,'start_ns':start,'done_observed_ns':end,'phase_clock_offset_spread_ms':(max(anchors)-min(anchors))/1e6,'timing':timing,'ingress_count':3,'ack_count':audit['acknowledged_writes'],'expected_observations':audit['expected_observations'],'readback_success':audit['audit']['success'],'audit_verified':audit['verified'],'post_done_paths':recovery,'dropped_capacity':sum(s['dropped_capacity'] for s in stages),'dropped_scheduler':sum(s['dropped_scheduler'] for s in stages),'driver_exit':next(s.split('LOAD process_exit=',1)[1] for _,s in markers if 'LOAD process_exit=' in s),'server_test_exit':0}

if __name__=='__main__':
    names=['A1','B1','B2','A2']
    runs={n:load(n) for n in names}
    assert all(v[-1]['config']==runs['A1'][-1]['config'] for v in runs.values())
    common=min(60*10**9,min(v[3] for v in runs.values()))//10**9*10**9
    assert common>0
    result={'protocol':json.loads((root/'protocol.json').read_text()),'primary_window_seconds':common/1e9,'baseline_runtime':'a9f318f16 / c664c80bb','candidate':json.loads((root/'candidate-ready.json').read_text()),'vm':{'id':150,'vcpus':8,'memory_gib':8,'sync':True,'profiles':False},'runs':{}}
    for n,(rows,start,done,_,data) in runs.items():
        data['split_common_rate']=stats(rows,start,start+common)
        data['post_done_8s_at_300']=stats(rows,done,done+8*10**9) if done+8*10**9<=data['stage_300_end_ns'] else None
        data['split_done_seconds']=(done-start)/1e9
        result['runs'][n]=data
        (root/(n+'-summary.json')).write_text(json.dumps(data,indent=2)+'\n')
    identities={d:json.loads((root.parent/d/'verified.json').read_text()) for d in ['final','replica']}
    for binary in ['baseline-server.test','candidate-server.test','loadtest-window']:
        assert identities['final'][binary]==identities['replica'][binary]
    result['binary_identities']=identities
    for a,b in [('A1','B1'),('A2','B2')]:
        print(a,b)
        for kind in ['read','write']:
            av=result['runs'][a]['split_common_rate'][kind]; bv=result['runs'][b]['split_common_rate'][kind]
            print(kind, av, bv)
    (root/'comparison.json').write_text(json.dumps(result,indent=2)+'\n')
