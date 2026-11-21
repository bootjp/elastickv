import importlib.util
import unittest
from unittest.mock import Mock, patch
from pathlib import Path

spec = importlib.util.spec_from_file_location("isolation", Path(__file__).with_name("isolate-load-host.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class FakeSystemd:
    def __init__(self):
        self.states = {
            unit: {"LoadState": "loaded", "UnitFileState": "static", "ActiveState": "inactive"}
            for unit in module.UNITS
        }
        self.states[module.UNITS[0]].update(UnitFileState="enabled", ActiveState="active")
        self.fail_mask = ""
        self.calls = []

    def state(self, unit):
        return dict(self.states[unit])

    def change(self, action, *args):
        unit = args[-1]
        self.calls.append((action, unit))
        state = self.states[unit]
        if action == "mask":
            state["UnitFileState"] = "masked-runtime"
            if unit == self.fail_mask:
                raise RuntimeError("partial mask failure")
        elif action == "unmask":
            state["UnitFileState"] = "enabled" if unit.endswith(".timer") else "static"
        else:
            state["ActiveState"] = "active" if action == "start" else "inactive"


class IsolationTest(unittest.TestCase):
    def run_isolated(self, controller, body=lambda: None):
        record = {}
        isolation = module.Isolation(controller, record.update)
        try:
            isolation.acquire()
            body()
        finally:
            isolation.release()
        return record

    def test_restore_after_command_failure(self):
        controller = FakeSystemd()
        before = {u: controller.state(u) for u in module.UNITS}

        def fail():
            self.assertTrue(all(s["UnitFileState"] == "masked-runtime" for s in controller.states.values()))
            raise RuntimeError("benchmark failed")

        with self.assertRaisesRegex(RuntimeError, "benchmark failed"):
            self.run_isolated(controller, fail)
        self.assertEqual(controller.states, before)

    def test_partial_mask_failure_is_unwound(self):
        controller = FakeSystemd()
        before = {u: controller.state(u) for u in module.UNITS}
        controller.fail_mask = module.UNITS[1]
        with self.assertRaisesRegex(RuntimeError, "partial mask failure"):
            self.run_isolated(controller)
        self.assertEqual(controller.states, before)

    def test_preexisting_masks_remain(self):
        for state in ("masked", "masked-runtime"):
            with self.subTest(state=state):
                controller = FakeSystemd()
                controller.states[module.UNITS[2]]["UnitFileState"] = state
                record = self.run_isolated(controller)
                self.assertEqual(record["restored"][module.UNITS[2]]["UnitFileState"], state)
                self.assertNotIn(("unmask", module.UNITS[2]), controller.calls)

    def test_running_maintenance_is_rejected(self):
        controller = FakeSystemd()
        controller.states[module.UNITS[2]]["ActiveState"] = "active"
        with self.assertRaisesRegex(RuntimeError, "wait for maintenance"):
            self.run_isolated(controller)
        self.assertEqual(controller.calls, [])

    def test_interrupt_restores_timer(self):
        controller = FakeSystemd()
        before = {u: controller.state(u) for u in module.UNITS}

        def interrupt():
            raise KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            self.run_isolated(controller, interrupt)
        self.assertEqual(controller.states, before)

    def test_child_stops_before_isolation_is_released(self):
        controller = FakeSystemd()
        child = Mock(pid=1234)
        child.wait.side_effect = [KeyboardInterrupt(), 143]
        with patch.object(module.subprocess, "Popen", return_value=child), patch.object(module.os, "killpg") as kill:
            with self.assertRaises(KeyboardInterrupt):
                self.run_isolated(controller, lambda: module.run_command(["benchmark"]))
            kill.assert_called_once_with(1234, module.signal.SIGTERM)
            self.assertEqual(child.wait.call_count, 2)
        self.assertEqual(controller.states[module.UNITS[0]]["ActiveState"], "active")


if __name__ == "__main__":
    unittest.main()
