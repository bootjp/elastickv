#!/usr/bin/env python3
"""Regression for draining large benchmark samples before worker join."""

import importlib.util
import multiprocessing
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).with_name("benchmark-fuse.py")
SPEC = importlib.util.spec_from_file_location("benchmark_fuse", SCRIPT)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def send_large_result(queue):
    queue.put((list(range(30000)), None))


class BenchmarkWorkerResultsTest(unittest.TestCase):
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
