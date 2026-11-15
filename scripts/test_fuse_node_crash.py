#!/usr/bin/env python3
"""Validate restart arguments for strict-only filesystem nodes."""
import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location("fuse_node_crash", pathlib.Path(__file__).with_name("test-fuse-node-crash.py"))
crash = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crash)


class RestartArgumentsTest(unittest.TestCase):
    def test_default_mode_preserves_empty_arguments(self):
        original = ["server", "--metricsAddress", "", "--filesystemMount", "/mnt/fs"]
        self.assertEqual(original + ["--filesystemStrictRecover"], crash.restart_arguments(original))
        self.assertEqual("/mnt/fs", original[-1])

    def test_true_compatibility_flags_are_supported(self):
        for arg in ("--filesystemStrict", "--filesystemStrict=true", "--filesystemStrict=1"):
            self.assertEqual(["server", arg, "--filesystemStrictRecover"], crash.restart_arguments(["server", arg]))

    def test_false_mode_is_rejected(self):
        for value in ("false", "FALSE", "0", "f"):
            with self.assertRaisesRegex(AssertionError, "legacy filesystem mode"):
                crash.restart_arguments(["server", "--filesystemStrict=" + value])

    def test_recovery_is_enabled_after_an_explicit_false(self):
        self.assertEqual(["server", "--filesystemStrictRecover=false", "--filesystemStrictRecover"],
                         crash.restart_arguments(["server", "--filesystemStrictRecover=false"]))


if __name__ == "__main__":
    unittest.main()
