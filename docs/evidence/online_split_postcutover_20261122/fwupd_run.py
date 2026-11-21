import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path

root = Path(__file__).resolve().parent
out = root / sys.argv[1]
out.mkdir()
endpoint = ""
done_ns = 0
burst_started = False
threads = []
sync_processes = []
stopping = threading.Event()

def capture(name, suffix):
    started = time.time_ns()
    try:
        with urllib.request.urlopen(endpoint + suffix, timeout=35) as response:
            (out / (name + ".pprof")).write_bytes(response.read())
    except Exception as err:
        (out / (name + ".error")).write_text(str(err))
    with (out / "profile-windows.jsonl").open("a") as f:
        print(json.dumps({"name": name, "started_ns": started, "ended_ns": time.time_ns()}), file=f)

def tail_profile(stage_start):
    remaining = stage_start + 75 - time.time()
    if remaining > 0:
        stopping.wait(remaining)
    if stopping.is_set():
        return
    for kind in ["mutex", "block", "heap"]:
        capture("before-" + kind, kind)
    capture("tail-cpu", "profile?seconds=15")
    for kind in ["mutex", "block", "heap"]:
        capture("after-" + kind, kind)

def stacks(stage_start):
    if stopping.wait(max(0, stage_start + 82 - time.time())):
        return
    capture("tail-goroutine", "goroutine?debug=2")

def burst_cpu():
    capture("burst-cpu", "profile?seconds=10")
    capture("burst-heap", "heap")

def burst_stacks():
    for i in range(3):
        capture("burst-goroutine-" + str(i), "goroutine?debug=2")
        stopping.wait(0.5)

def inject_fwupd(stage_start):
    if stopping.wait(max(0, stage_start + 78 - time.time())):
        return
    started = time.time_ns()
    with (out / "fwupd-injection.log").open("w", buffering=1) as f:
        print(json.dumps({"start_ns":started}), file=f)
        subprocess.run(["sudo", "-n", "systemctl", "start", "fwupd-refresh.service"], stdout=f, stderr=subprocess.STDOUT, check=True)
        print(json.dumps({"end_ns":time.time_ns()}), file=f)

def sync_records(pid):
    command = ["sudo", "-n", "bpftrace", "-B", "none", "-p", str(pid), str(root / "sync_latency.bt"), str(pid)]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
    sync_processes.append(proc)
    with (out / "sync-latency.jsonl").open("w", buffering=1) as file:
        for line in proc.stdout:
            now = time.time_ns()
            text = line.decode(errors="replace").strip()
            match = re.search(r"PROFILE_POSTCUTOVER_SYNC (\d+) (\d+) (\d+) (\d+)", text)
            path = ""
            if match:
                try:
                    path = os.readlink(f"/proc/{pid}/fd/{match[3]}")
                except OSError:
                    pass
            print(json.dumps({"time_ns": now, "text": text, "path": path}), file=file)
    proc.wait()

def pressure(pid):
    with (out / "pressure.jsonl").open("w", buffering=1) as f:
        while not stopping.is_set():
            record = {"time_ns": time.time_ns()}
            for name in ["io", "cpu", "memory"]:
                record[name] = Path("/proc/pressure/" + name).read_text()
            record["stat"] = Path("/proc/stat").read_text().splitlines()[0]
            record["disk"] = [s for s in Path("/proc/diskstats").read_text().splitlines() if " vda " in s]
            try:
                record["process_io"] = Path(f"/proc/{pid}/io").read_text()
            except OSError:
                break
            print(json.dumps(record), file=f)
            stopping.wait(1)

env = {k: v for k, v in os.environ.items() if not k.startswith("ELASTICKV_")}
env.update(GOMAXPROCS="8", GODEBUG="gctrace=1", ELASTICKV_LOAD_BINARY=str(root / "burst-driver-no-gctrace"), ELASTICKV_LOAD_RAMP="1", ELASTICKV_PERF_PREFLIGHT_TIMING="1", ELASTICKV_LOAD_RATES="100,300", ELASTICKV_LOAD_STAGE_DURATION="90s", ELASTICKV_LOAD_CONCURRENCY="32", ELASTICKV_LOAD_READ_PERCENT="50", ELASTICKV_LOAD_SPLIT_STAGE="1", ELASTICKV_LOAD_SPLIT="cross", ELASTICKV_RPC_TRACE=str(out / "rpc.csv"), ELASTICKV_LOAD_REQUIRE_ONLINE="1", ELASTICKV_LOAD_PROFILE_DIR=str(out), ELASTICKV_LOAD_PROFILE_ONLY="manual", ELASTICKV_DEBUG_LSM_EVENTS="1")
pid = subprocess.check_output(["systemctl", "show", "fwupd.service", "--value", "-p", "MainPID"], text=True).strip()
files = set()
if pid != "0":
    for line in subprocess.check_output(["sudo", "-n", "cat", f"/proc/{pid}/maps"], text=True).splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[5].startswith("/"):
            files.add(parts[5])
subprocess.run(["sudo", "-n", "systemctl", "stop", "fwupd.service"], check=True)
for dirname in ["/var/lib/fwupd", "/var/cache/fwupd", "/var/cache/fwupdmgr", "/usr/share/fwupd", "/usr/libexec/fwupd"]:
    files.update(str(f) for f in Path(dirname).rglob("*") if f.is_file())
evicted = []
for name in sorted(files):
    try:
        with open(name, "rb") as f:
            os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        evicted.append(name)
    except OSError:
        pass
(out / "fwupd-evicted.json").write_text(json.dumps(evicted, indent=2))
monitors = []
with (out / "run.log").open("xb") as raw, (out / "markers.jsonl").open("x", buffering=1) as timeline:
    proc = subprocess.Popen([str(root / "burst-server.test"), "-test.run=^TestLinuxSplitLoadRamp$", "-test.count=1", "-test.v", "-test.timeout=15m"], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    thread = threading.Thread(target=pressure, args=(proc.pid,))
    thread.start()
    threads.append(thread)
    sync_thread = threading.Thread(target=sync_records, args=(proc.pid,))
    sync_thread.start()
    threads.append(sync_thread)
    for name, command in [("pidstat", ["pidstat", "-durh", "-p", "ALL", "1"]), ("iostat", ["iostat", "-x", "-d", "-y", "-t", "1"])]:
        handle = (out / (name + ".log")).open("xb")
        monitors.append((subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT), handle))
    for line in proc.stdout:
        ns = time.time_ns()
        raw.write(line)
        text = line.decode(errors="replace")
        if "LOAD " in text or "PERF " in text or "PROFILE_POSTCUTOVER" in text or text.startswith("gc "):
            raw.flush()
            print(json.dumps([ns, text.strip()]), file=timeline)
        match = re.search(r"profile_endpoint=(\S+)", text)
        if match:
            endpoint = match[1]
        if '"type":"stage_start"' in text:
            e = json.loads(text.split("LOAD ", 1)[1])
            if e["index"] == 1:
                stage_start = datetime.fromisoformat(e["time"].replace("Z", "+00:00")).timestamp()
                thread = threading.Thread(target=inject_fwupd, args=(stage_start,))
                thread.start()
                threads.append(thread)
        if re.search(r"PERF migration .* phase=7(?: |$)", text):
            done_ns = ns
        match = re.search(r"PROFILE_POSTCUTOVER_RPC (\d+) (\w+) (\d+) (\d+) (\w+)", text)
        if match and done_ns and ns > done_ns + 5 * 10**9 and not burst_started:
            elapsed = int(match[4])
            if elapsed > (100 if match[2] == "read" else 200) * 10**6:
                burst_started = True
                for target in [burst_cpu, burst_stacks]:
                    thread = threading.Thread(target=target)
                    thread.start()
                    threads.append(thread)
    rc = proc.wait()
    stopping.set()
    for trace in sync_processes:
        subprocess.run(["sudo", "-n", "kill", "-INT", "--", "-" + str(trace.pid)], check=False)
    for thread in threads:
        thread.join(timeout=40)
    for monitor, handle in monitors:
        monitor.terminate()
        monitor.wait()
        handle.close()
    (out / "exit.txt").write_text(str(rc))
print("diagnostic exit", rc, flush=True)
