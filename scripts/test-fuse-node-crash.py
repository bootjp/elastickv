#!/usr/bin/env python3
"""Check strict FUSE fsync durability through one Raft node SIGKILL/restart."""

import argparse
import os
import pathlib
import signal
import subprocess
import time


def await_contents(path, expected, seconds=90):
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            actual = path.read_bytes()
            if actual == expected:
                return
            last_error = AssertionError(f"{path}: {actual!r} != {expected!r}")
        except OSError as exc:
            last_error = exc
        time.sleep(0.5)
    raise AssertionError(f"{path}: did not recover: {last_error}")


def sync_file_and_directory(path, contents):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(contents)
            stream.flush()
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mounts", nargs=3, type=pathlib.Path)
    parser.add_argument("--pid-file", type=pathlib.Path, required=True)
    parser.add_argument("--restart-log", type=pathlib.Path, required=True)
    args = parser.parse_args()

    name = "strict-node-crash.dat"
    contents = b"committed before Raft node SIGKILL\n" * 128
    paths = [mount / name for mount in args.mounts]
    sync_file_and_directory(paths[0], contents)
    for path in paths[1:]:
        await_contents(path, contents)

    pid = int(args.pid_file.read_text().splitlines()[0])
    # /proc cmdline ends in NUL; interior empty arguments (for example an
    # intentionally disabled metrics address) must remain in their positions.
    argv = [os.fsdecode(arg) for arg in pathlib.Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")[:-1]]
    if not argv or "--filesystemStrict" not in argv:
        raise AssertionError("node was not started in strict mode")
    os.kill(pid, signal.SIGKILL)
    # The surviving quorum must still serve the acknowledged file.
    for path in paths[1:]:
        await_contents(path, contents)

    with args.restart_log.open("wb") as log:
        restarted = subprocess.Popen(argv + ["--filesystemStrictRecover"], stdout=log, stderr=subprocess.STDOUT)
    with args.pid_file.open("a") as pid_file:
        pid_file.write(f"{restarted.pid}\n")
    await_contents(paths[0], contents)
    if restarted.poll() is not None:
        raise AssertionError(f"restarted node exited with {restarted.returncode}")
    print("PASS: strict FUSE file and directory fsync survived node SIGKILL and restart")


if __name__ == "__main__":
    main()
