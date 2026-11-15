#!/usr/bin/env python3
"""Regression for draining large benchmark samples before worker join."""

import importlib.util
import multiprocessing
import pathlib
import unittest
import tempfile
import json
import sys
import io
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("benchmark-fuse.py")
SPEC = importlib.util.spec_from_file_location("benchmark_fuse", SCRIPT)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def send_large_result(queue):
    queue.put((list(range(30000)), None))


class BenchmarkWorkerResultsTest(unittest.TestCase):
    def run_with_mode(self, mode):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "results.json"
            argv = [str(SCRIPT), directory, "--layout", "single-group", "--output", str(output)]
            if mode is not None:
                argv += ["--mode", mode]
            with mock.patch.object(sys, "argv", argv), \
                 mock.patch.object(benchmark, "run_serial", return_value={}), \
                 mock.patch.object(benchmark, "run_hot", return_value={}), \
                 mock.patch("builtins.print"), \
                 mock.patch.object(sys, "stderr", io.StringIO()):
                benchmark.main()
            return json.loads(output.read_text())

    def test_default_mode_is_strict(self):
        self.assertEqual("strict", self.run_with_mode(None)["mode"])

    def test_legacy_mode_is_rejected(self):
        with self.assertRaises(SystemExit) as result:
            self.run_with_mode("legacy")
        self.assertEqual(2, result.exception.code)

    def test_write_failure_identifies_syscall_and_offset(self):
        with mock.patch.object(benchmark.os, "pwrite", side_effect=OSError(5, "Input/output error")):
            with self.assertRaisesRegex(OSError, "pwrite offset=4096 length=4"):
                benchmark.write_and_sync(1, 4096, b"data")

    def test_fsync_failure_identifies_syscall_and_offset(self):
        with mock.patch.object(benchmark.os, "pwrite", return_value=4), \
             mock.patch.object(benchmark.os, "fsync", side_effect=OSError(5, "Input/output error")):
            with self.assertRaisesRegex(OSError, "fsync after offset=4096 length=4"):
                benchmark.write_and_sync(1, 4096, b"data")

    def test_large_sample_queue_does_not_block_worker_exit(self):
        queue = multiprocessing.Queue()
        processes = [multiprocessing.Process(target=send_large_result, args=(queue,)) for _ in range(2)]
        for process in processes:
            process.start()
        outcomes = benchmark.collect_worker_results(processes, queue, timeout=10)
        self.assertEqual([30000, 30000], [len(samples) for samples, _ in outcomes])
        self.assertTrue(all(process.exitcode == 0 for process in processes))


if __name__ == "__main__":
    unittest.main()
