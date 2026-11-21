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
    assert phase[-1][2]==7 and len(recovery)==5 and all(a==[600,600,600,0] for a in recovery.values())
    anchors=[ns-ms*10**6 for ns,ms,_ in phase]
    end,elapsed,_=phase[-1]
    start=end-elapsed*10**6
    last300=parse_timestamp(stages[1]['started'])+cfg['stage_duration_ns']
    assert parse_timestamp(stages[1]['started'])<start<last300
    totals={}
    for kind in ['read','write']:
        err=Counter()
        for s in stages:err.update(s[kind]['errors'] or {})
        totals[kind]={'success':sum(s[kind]['success'] for s in stages),'errors':dict(err)}
    if name=='candidate':assert timing['capture_version']==1 and timing['sequence']==timing['seal_sequence']>0
    return rows,start,end,min(end,last300)-start,{'config':cfg,'split_all':stats(rows,start,end),'full_run':totals,'start_ns':start,'done_observed_ns':end,'phase_clock_offset_spread_ms':(max(anchors)-min(anchors))/1e6,'timing':timing,'ingress_count':3,'ack_count':audit['acknowledged_writes'],'expected_observations':audit['expected_observations'],'readback_success':audit['audit']['success'],'audit_verified':audit['verified'],'post_done_paths':recovery,'dropped_capacity':sum(s['dropped_capacity'] for s in stages),'dropped_scheduler':sum(s['dropped_scheduler'] for s in stages),'driver_exit':next(s.split('LOAD process_exit=',1)[1] for _,s in markers if 'LOAD process_exit=' in s),'server_test_exit':0}
if __name__=='__main__':
    if len(sys.argv)>1:
        for name in sys.argv[1:]:
            print(json.dumps(load(name)[-1],indent=2))
    else:
        runs={name:load(name) for name in ['baseline','candidate']}
        assert runs['baseline'][-1]['config']==runs['candidate'][-1]['config']
        common=min(v[3] for v in runs.values())//10**9*10**9
        assert common>0
        result={'binary_sha256':json.loads((root/'expected.json').read_text()),'source_commits':{'baseline':'8c2282837ecee54d51c664c67576a8f90e5f90e4','candidate':json.loads((root/'candidate-ready.json').read_text())['source_commit']},'vm':{'id':150,'vcpus':8,'memory_gib':8,'nodes':3,'replication_groups':2,'sync_persistence':True,'profiling':False},'common_duration_seconds':common/1e9,'method':'Each cross-group window starts at the reported migration request start, anchored to the streamed Done observation (phase polling at 100ms). Count client RPC completions within the window. p99 uses successful single primary RPCs, excluding verification and audit. Common rate comparison covers equal whole seconds after request start within stage 300. Completed earlier baseline versus fresh fixed candidate; the intervening profiled/intermediate runs and failed fresh baseline retry are recorded separately. No mutation retries and no statistical significance claim.'}
        for name,(rows,t0,t1,_,data) in runs.items():
            data['split_common_rate']=stats(rows,t0,t0+common)
            result[name]=data
        (root/'comparison.json').write_text(json.dumps(result,indent=2)+'\n')
        for n in runs:
            for k in ('split_all','split_common_rate','ack_count','readback_success','timing','phase_clock_offset_spread_ms','full_run','dropped_capacity','dropped_scheduler'):print(n,k,result[n][k])
