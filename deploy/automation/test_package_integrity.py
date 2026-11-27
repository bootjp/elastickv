"""Exercise the Docker install boundary with an attacker-substituted wheel."""

import hashlib
import pathlib
import shlex
import subprocess
import sys
import tempfile
import unittest
import zipfile

import prepare
import settings
import yaml


class PackageIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.wheel = self.root / "guard_integrity_probe-1.0-py3-none-any.whl"
        with zipfile.ZipFile(self.wheel, "w") as archive:
            archive.writestr("guard_integrity_probe.py", 'VALUE = "replacement"\n')
            archive.writestr(
                "guard_integrity_probe-1.0.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: guard-integrity-probe\nVersion: 1.0\n",
            )
            archive.writestr(
                "guard_integrity_probe-1.0.dist-info/WHEEL",
                "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
            )
            archive.writestr("guard_integrity_probe-1.0.dist-info/RECORD", "")
        self.digest = hashlib.sha256(self.wheel.read_bytes()).hexdigest()

    def install(self, requirement):
        requirements = self.root / "requirements.txt"
        requirements.write_text(requirement + "\n")
        dockerfile = (settings.PACKAGE / "Dockerfile").read_text()
        command = next(
            line.removeprefix("RUN ")
            for line in dockerfile.splitlines()
            if line.startswith("RUN pip install ")
        )
        args = shlex.split(command)[2:]
        args[args.index("-r") + 1] = str(requirements)
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                *args,
                "--disable-pip-version-check",
                "--no-index",
                "--find-links",
                str(self.root),
                "--target",
                str(self.root / "installed"),
            ],
            capture_output=True,
            text=True,
        )

    def test_unhashed_version_pin_cannot_authorize_replacement_bytes(self):
        result = self.install("guard-integrity-probe==1.0")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Hashes are required", result.stdout + result.stderr)
        self.assertFalse((self.root / "installed/guard_integrity_probe.py").exists())

    def test_wrong_trusted_hash_rejects_the_artifact_before_install(self):
        result = self.install("guard-integrity-probe==1.0 --hash=sha256:" + "0" * 64)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("DO NOT MATCH THE HASHES", result.stdout + result.stderr)
        self.assertFalse((self.root / "installed/guard_integrity_probe.py").exists())

    def test_matching_trusted_hash_preserves_legitimate_install(self):
        result = self.install("guard-integrity-probe==1.0 --hash=sha256:" + self.digest)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue((self.root / "installed/guard_integrity_probe.py").is_file())

    def test_rendered_privileged_workflows_keep_the_same_hash_lock(self):
        output = self.root / "rendered"
        prepare.render({**settings.DEFAULTS, "code_path": "release-tools"}, output)
        canonical = (settings.PACKAGE / "requirements-parser.lock").read_bytes()
        self.assertEqual(
            (output / "release-tools/requirements-parser.lock").read_bytes(), canonical
        )
        installs = 0
        for path in (output / ".github/workflows").glob("*.yml"):
            workflow = yaml.safe_load(path.read_text())
            for job in workflow["jobs"].values():
                for step in job["steps"]:
                    for line in step.get("run", "").splitlines():
                        if "pip install" not in line:
                            continue
                        args = shlex.split(line)
                        self.assertIn("--require-hashes", args)
                        self.assertIn("--only-binary=:all:", args)
                        lock = output / args[args.index("-r") + 1]
                        self.assertEqual(lock.read_bytes(), canonical)
                        installs += 1
        self.assertEqual(installs, 5)

    def test_runtime_lock_keeps_the_declared_input_pins(self):
        locked = (settings.PACKAGE / "requirements.lock").read_text() + (
            settings.PACKAGE / "requirements-parser.lock"
        ).read_text()
        for line in (settings.PACKAGE / "requirements.txt").read_text().splitlines():
            if line and not line.startswith("#"):
                self.assertIn(line + " \\", locked)


if __name__ == "__main__":
    unittest.main()
