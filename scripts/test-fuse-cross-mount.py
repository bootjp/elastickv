#!/usr/bin/env python3
"""Exercise disjoint writes to one FUSE chunk through different mounts."""

import argparse
import multiprocessing
import os
import pathlib
import queue
import uuid


def write_one(path, offset, data, start, results):
    stage = "wait"
    try:
        if not start.wait(10):
            raise TimeoutError("writer start timed out")
        stage = "open"
        fd = os.open(path, os.O_WRONLY)
        try:
            stage = "pwrite"
            written = os.pwrite(fd, data, offset)
            if written != len(data):
                raise OSError(f"short write: {written} of {len(data)} bytes")
            stage = "fsync"
            os.fsync(fd)
        finally:
            os.close(fd)
        results.put(None)
    except Exception as exc:  # The parent reports errors from each child.
        results.put(f"{stage}: {exc!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mounts", nargs=3, type=pathlib.Path)
    parser.add_argument("--iterations", type=int, default=20)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")

    prefix = f"disjoint-{uuid.uuid4().hex}"
    for index in range(args.iterations):
        name = f"{prefix}-{index}"
        (args.mounts[0] / name).write_bytes(b"00")
        start = multiprocessing.Event()
        results = multiprocessing.Queue()
        writers = [
            multiprocessing.Process(
                target=write_one,
                args=(str(args.mounts[mount] / name), mount, data, start, results),
            )
            for mount, data in [(0, b"A"), (1, b"B")]
        ]
        for writer in writers:
            writer.start()
        start.set()
        for writer in writers:
            writer.join(30)
            if writer.is_alive():
                writer.terminate()
                writer.join()
                raise TimeoutError(f"iteration {index}: writer timed out")
        try:
            errors = [results.get(timeout=1) for _ in writers]
        except queue.Empty as exc:
            raise RuntimeError(f"iteration {index}: missing writer result") from exc
        if any(errors):
            raise RuntimeError(f"iteration {index}: writer errors: {errors}")
        actual = (args.mounts[2] / name).read_bytes()
        if actual != b"AB":
            raise AssertionError(f"iteration {index}: expected AB, got {actual!r}")
    print(f"PASS: {args.iterations} cross-mount disjoint writes and fsyncs")


if __name__ == "__main__":
    main()
