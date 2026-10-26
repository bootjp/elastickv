#!/usr/bin/env python3
"""Check strict FUSE fsync durability through one Raft node SIGKILL/restart."""

import argparse
import os
import pathlib
import signal
import stat
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
    sync_directory(path.parent)


def sync_directory(path):
    directory = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def await_mutation_state(mount, contents):
    await_contents(mount / "strict-node-crash.dat", contents)
    await_contents(mount / "strict-node-renamed.dat", b"renamed across a node crash\n")
    await_contents(mount / "strict-node-directory" / "truncated.dat", b"truncated\n")
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        renamed = mount / "strict-node-renamed.dat"
        removed = mount / "strict-node-removed.dat"
        empty = mount / "strict-node-empty"
        nested = mount / "strict-node-directory"
        try:
            if (stat.S_IMODE(renamed.stat().st_mode) == 0o640
                    and nested.is_dir() and not removed.exists() and not empty.exists()
                    and not (mount / "strict-node-source.dat").exists()):
                return
        except OSError:
            pass
        time.sleep(0.5)
    raise AssertionError(f"{mount}: mutation state did not recover")


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
    first = args.mounts[0]
    source = first / "strict-node-source.dat"
    renamed = first / "strict-node-renamed.dat"
    sync_file_and_directory(source, b"renamed across a node crash\n")
    os.rename(source, renamed)
    os.chmod(renamed, 0o640)
    with renamed.open("rb") as stream:
        os.fsync(stream.fileno())
    removed = first / "strict-node-removed.dat"
    sync_file_and_directory(removed, b"remove me\n")
    os.unlink(removed)
    empty = first / "strict-node-empty"
    os.mkdir(empty)
    os.rmdir(empty)
    nested = first / "strict-node-directory"
    os.mkdir(nested)
    truncated = nested / "truncated.dat"
    sync_file_and_directory(truncated, b"truncated\nthen removed\n")
    os.truncate(truncated, len(b"truncated\n"))
    with truncated.open("rb") as stream:
        os.fsync(stream.fileno())
    sync_directory(nested)
    sync_directory(first)
    for mount in args.mounts[1:]:
        await_mutation_state(mount, contents)

    pids = [int(line) for line in args.pid_file.read_text().splitlines() if line.strip()]
    if len(pids) < len(args.mounts):
        raise AssertionError("pid file does not contain every cluster node")
    pid = pids[0]
    # /proc cmdline ends in NUL; interior empty arguments (for example an
    # intentionally disabled metrics address) must remain in their positions.
    argv = [os.fsdecode(arg) for arg in pathlib.Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")[:-1]]
    if not argv or "--filesystemStrict" not in argv:
        raise AssertionError("node was not started in strict mode")
    os.kill(pid, signal.SIGKILL)
    # The surviving quorum must still serve the acknowledged file.
    for mount in args.mounts[1:]:
        await_mutation_state(mount, contents)

    restart_argv = argv if "--filesystemStrictRecover" in argv else argv + ["--filesystemStrictRecover"]
    with args.restart_log.open("wb") as log:
        restarted = subprocess.Popen(restart_argv, stdout=log, stderr=subprocess.STDOUT)
    with args.pid_file.open("a") as pid_file:
        pid_file.write(f"{restarted.pid}\n")
    await_mutation_state(args.mounts[0], contents)
    for _ in range(10):
        if restarted.poll() is not None:
            raise AssertionError(f"restarted node exited with {restarted.returncode}")
        time.sleep(0.5)
    print("PASS: acknowledged FUSE mutations and fsync survived node SIGKILL and restart")


if __name__ == "__main__":
    main()
