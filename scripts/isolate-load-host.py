#!/usr/bin/env python3
"""Run a load comparison on a dedicated Linux host without firmware probes.

Usage: isolate-load-host.py OUTPUT_DIRECTORY -- COMMAND [ARG ...]
Only runtime masks are added; every pre-existing mask and active state is kept.
The command and its ACK audit must both finish before this wrapper returns.
"""

import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

UNITS = ("fwupd-refresh.timer", "fwupd-refresh.service", "fwupd.service")


class Systemd:
    def state(self, unit):
        result = subprocess.run(
            ["systemctl", "show", unit, "-p", "LoadState", "-p", "UnitFileState", "-p", "ActiveState"],
            text=True, check=True, capture_output=True,
        )
        return dict(line.split("=", 1) for line in result.stdout.splitlines())

    def change(self, *args):
        subprocess.run(["sudo", "-n", "systemctl", *args], check=True)


class Isolation:
    def __init__(self, systemd, save):
        self.systemd, self.save = systemd, save
        self.before, self.masked, self.stopped = {}, [], []

    def acquire(self):
        self.before = {unit: self.systemd.state(unit) for unit in UNITS}
        self.save({"before": self.before})
        for unit, state in self.before.items():
            if state["LoadState"] == "not-found":
                continue
            if state["ActiveState"] not in ("active", "inactive", "failed"):
                raise RuntimeError(f"{unit} is changing state; retry after maintenance completes")
            if state["ActiveState"] == "active" and unit.endswith(".service"):
                raise RuntimeError(f"{unit} is active; wait for maintenance to finish")
        for unit, state in self.before.items():
            if state["LoadState"] == "not-found":
                continue
            if not state["UnitFileState"].startswith("masked"):
                # Record intent before the call: a failed systemctl may still
                # have created the mask, which must be removed on unwind.
                self.masked.append(unit)
                self.systemd.change("mask", "--runtime", unit)
            if state["ActiveState"] == "active":
                self.stopped.append(unit)
                self.systemd.change("stop", unit)
        current = {unit: self.systemd.state(unit) for unit in self.before}
        for unit, state in current.items():
            if state["LoadState"] == "not-found":
                continue
            if state["ActiveState"] not in ("inactive", "failed") or not state["UnitFileState"].startswith("masked"):
                raise RuntimeError(f"{unit} did not become inactive and masked")
        self.save({"isolated": current, "started_ns": time.time_ns()})

    def release(self):
        failures = []
        for unit in reversed(self.masked):
            try:
                self.systemd.change("unmask", "--runtime", unit)
            except Exception as error:
                failures.append(f"unmask {unit}: {error}")
        for unit in self.stopped:
            try:
                self.systemd.change("start", unit)
            except Exception as error:
                failures.append(f"start {unit}: {error}")
        self.save({"restored": {unit: self.systemd.state(unit) for unit in self.before}, "restore_errors": failures})
        if failures:
            raise RuntimeError("; ".join(failures))


def run_command(command):
    process = subprocess.Popen(command, start_new_session=True)
    try:
        return process.wait()
    except BaseException:
        # Keep isolation until the command and all of its descendants stop.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        raise


def main():
    if len(sys.argv) < 4 or sys.argv[2] != "--" or sys.platform != "linux":
        raise SystemExit(__doc__)
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    record = {"command": sys.argv[3:]}

    def save(update):
        record.update(update)
        (out / "host-isolation.json").write_text(json.dumps(record, indent=2) + "\n")

    def interrupted(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    with Path("/var/tmp/elastickv-load-host.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        isolation = Isolation(Systemd(), save)
        try:
            isolation.acquire()
            result = run_command(sys.argv[3:])
            save({"ended_ns": time.time_ns(), "exit_code": result})
        finally:
            isolation.release()
    return result


if __name__ == "__main__":
    sys.exit(main())
