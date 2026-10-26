#!/usr/bin/env python3
"""Exercise SQLite rollback journals with multiple processes on one mount."""

import argparse
from contextlib import closing
import multiprocessing
import pathlib
import sqlite3
import uuid


BUSY_TIMEOUT_SECONDS = 90
WRITER_JOIN_SECONDS = 300


def increment(path, mode, writes, start, results):
    phase = "open"
    try:
        with closing(sqlite3.connect(path, timeout=BUSY_TIMEOUT_SECONDS)) as connection:
            # PERSIST is connection-local, so each writer must select it.
            # Configure the connections before either starts writing.
            phase = "configure journal mode"
            actual_mode = connection.execute(f"PRAGMA journal_mode={mode}").fetchone()[0]
            if actual_mode.upper() != mode:
                raise AssertionError(f"journal mode {actual_mode}, expected {mode}")
            connection.execute("PRAGMA synchronous=FULL")
            results.put("ready")
            phase = "wait for writers"
            if not start.wait(BUSY_TIMEOUT_SECONDS):
                raise TimeoutError("writer start timed out")
            for index in range(writes):
                phase = f"write {index + 1}/{writes}"
                connection.execute("BEGIN IMMEDIATE")
                connection.execute("UPDATE counter SET value=value+1")
                connection.commit()
        results.put(None)
    except Exception as exc:  # The parent reports the exact worker failure.
        results.put(f"{phase}: {exc!r}")


def run_mode(mount, mode, writers, writes):
    path = mount / f"sqlite-{mode.lower()}-{uuid.uuid4().hex}.db"
    with closing(sqlite3.connect(path, timeout=10)) as connection:
        actual_mode = connection.execute(f"PRAGMA journal_mode={mode}").fetchone()[0]
        if actual_mode.upper() != mode:
            raise AssertionError(f"journal mode {actual_mode}, expected {mode}")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("CREATE TABLE counter(value INTEGER NOT NULL)")
        connection.execute("INSERT INTO counter VALUES(0)")
        connection.commit()

    start = multiprocessing.Event()
    results = multiprocessing.Queue()
    processes = [
        multiprocessing.Process(target=increment, args=(str(path), mode, writes, start, results))
        for _ in range(writers)
    ]
    for process in processes:
        process.start()
        ready = results.get(timeout=BUSY_TIMEOUT_SECONDS)
        if ready != "ready":
            raise RuntimeError(f"{mode}: writer setup failed: {ready}")
    start.set()
    for process in processes:
        process.join(WRITER_JOIN_SECONDS)
        if process.is_alive():
            process.terminate()
            process.join()
            raise TimeoutError(f"{mode}: writer timed out")
    failures = [results.get(timeout=1) for _ in processes]
    if any(failures):
        raise RuntimeError(f"{mode}: writer errors: {failures}")
    with closing(sqlite3.connect(path, timeout=10)) as connection:
        value = connection.execute("SELECT value FROM counter").fetchone()[0]
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    if value != writers * writes or integrity != "ok":
        raise AssertionError(f"{mode}: value={value}, integrity={integrity}")
    print(f"PASS: {mode} value={value} integrity={integrity} path={path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mount", type=pathlib.Path)
    parser.add_argument("--writers", type=int, default=2)
    parser.add_argument("--writes", type=int, default=30)
    args = parser.parse_args()
    if args.writers < 1 or args.writes < 1:
        parser.error("writers and writes must be positive")
    for mode in ("PERSIST", "DELETE"):
        run_mode(args.mount, mode, args.writers, args.writes)


if __name__ == "__main__":
    main()
