#!/usr/bin/env python3
"""Measure strict FUSE write plus fsync latency on one file and a hot inode."""

import argparse
import json
import multiprocessing
import os
import pathlib
import time
import uuid


BLOCK = 4096


def percentile(samples, percent):
    ordered = sorted(samples)
    index = max(0, (len(ordered) * percent + 99) // 100 - 1)
    return round(ordered[index] / 1_000_000, 3)


def summary(samples, elapsed):
    return {
        "operations": len(samples),
        "elapsed_seconds": round(elapsed, 3),
        "ops_per_second": round(len(samples) / elapsed, 3),
        "latency_ms": {
            "p50": percentile(samples, 50),
            "p95": percentile(samples, 95),
            "p99": percentile(samples, 99),
            "max": round(max(samples) / 1_000_000, 3),
        },
    }


def write_and_sync(fd, offset, payload):
    started = time.perf_counter_ns()
    written = os.pwrite(fd, payload, offset)
    if written != len(payload):
        raise OSError(f"short pwrite: {written}/{len(payload)}")
    os.fsync(fd)
    return time.perf_counter_ns() - started


def hot_writer(path, worker, writes, workers, start, results):
    samples = []
    try:
        fd = os.open(path, os.O_RDWR)
        try:
            if not start.wait(30):
                raise TimeoutError("hot writer start timed out")
            payload = bytes([worker + 1]) * BLOCK
            for index in range(writes):
                samples.append(write_and_sync(fd, (index * workers + worker) * BLOCK, payload))
        finally:
            os.close(fd)
        results.put((samples, None))
    except Exception as exc:
        results.put((samples, repr(exc)))


def run_serial(path, writes):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    samples = []
    try:
        started = time.perf_counter()
        for index in range(writes):
            samples.append(write_and_sync(fd, index * BLOCK, b"S" * BLOCK))
        elapsed = time.perf_counter() - started
    finally:
        os.close(fd)
    if path.read_bytes() != b"S" * BLOCK * writes:
        raise AssertionError("serial writes were not retained")
    return summary(samples, elapsed)


def run_hot(path, workers, writes):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    os.close(fd)
    start = multiprocessing.Event()
    results = multiprocessing.Queue()
    processes = [
        multiprocessing.Process(target=hot_writer, args=(str(path), worker, writes, workers, start, results))
        for worker in range(workers)
    ]
    for process in processes:
        process.start()
    started = time.perf_counter()
    start.set()
    for process in processes:
        process.join(300)
        if process.is_alive():
            process.terminate()
            process.join()
            raise TimeoutError("hot writer timed out")
    outcomes = [results.get(timeout=1) for _ in processes]
    failures = [error for _, error in outcomes if error]
    if failures:
        raise RuntimeError(f"hot writer errors: {failures}")
    samples = [sample for worker_samples, _ in outcomes for sample in worker_samples]
    elapsed = time.perf_counter() - started
    expected = b"".join(bytes([worker + 1]) * BLOCK for _ in range(writes) for worker in range(workers))
    if path.read_bytes() != expected:
        raise AssertionError("hot inode writes were not retained")
    return summary(samples, elapsed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mount", type=pathlib.Path)
    parser.add_argument("--writes", type=int, default=40)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if args.writes < 1 or not 1 <= args.workers <= 255:
        parser.error("writes must be positive and workers must be between 1 and 255")
    prefix = uuid.uuid4().hex
    result = {
        "block_bytes": BLOCK,
        "writes_per_worker": args.writes,
        "workers": args.workers,
        "serial": run_serial(args.mount / f"benchmark-serial-{prefix}.dat", args.writes),
        "hot_inode": run_hot(args.mount / f"benchmark-hot-{prefix}.dat", args.workers, args.writes),
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
