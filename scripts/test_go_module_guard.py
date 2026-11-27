"""Exercise Guard configuration boundaries without network or host changes."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]


class GoModuleGuardTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.bin = self.work / "bin"
        self.bin.mkdir()
        self.env = dict(os.environ)
        self.env.update(
            PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            GOENV=str(self.work / "goenv"),
            GOTOOLCHAIN="local",
            PROXY_TEST_REAL_GO=shutil.which("go") or "",
            PROXY_TEST_GO=str(self.bin / "go"),
        )

    def executable(self, name, source):
        path = self.bin / name
        path.write_text("#!/usr/bin/env python3\n" + textwrap.dedent(source))
        path.chmod(0o755)

    def test_canary_overrides_saved_private_module_exceptions(self):
        saved = (
            "GOPROXY=https://golang.flatt.tech\n"
            "GOPRIVATE=github.com/*\n"
            "GONOPROXY=github.com/*\n"
            "GONOSUMDB=github.com/*\n"
        )
        Path(self.env["GOENV"]).write_text(saved)
        for name in ("GOPROXY", "GOPRIVATE", "GONOPROXY", "GONOSUMDB"):
            self.env.pop(name, None)
        self.executable("go", """
            import os, subprocess, sys
            args = sys.argv[1:]
            if args[:1] == ['env']:
                sys.exit(subprocess.call([os.environ['PROXY_TEST_REAL_GO'], *args]))
            if args[:2] == ['mod', 'init']:
                for name in ['GOPRIVATE', 'GONOPROXY', 'GONOSUMDB']:
                    value = subprocess.check_output(
                        [os.environ['PROXY_TEST_REAL_GO'], 'env', name], text=True
                    ).strip()
                    if value != 'none':
                        sys.exit(f'{name} still uses saved exception: {value}')
            elif args[-1] == 'github.com/flatt-security/hola-takumi-go@v0.1.0':
                print('https://golang.flatt.tech/github.com/flatt-security/'
                      'hola-takumi-go/@v/v0.1.0.info: 403 Forbidden', file=sys.stderr)
                sys.exit(1)
        """)
        result = subprocess.run(
            ["bash", str(ROOT / "scripts/check-go-module-guard.sh")],
            cwd=self.work, env=self.env, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(Path(self.env["GOENV"]).read_text(), saved)

    def test_provision_uses_existing_go_outside_default_install_path(self):
        self.executable("go", """
            print('go version go1.25.5 linux/amd64')
        """)
        self.executable("dpkg", """
            print('amd64')
        """)
        self.executable("sudo", """
            import os, sys
            args = sys.argv[1:]
            if args[:1] == ['tee']:
                sys.stdin.read()
            elif args == ['-u', 'vagrant', '-H', os.environ['PROXY_TEST_GO'],
                          'env', '-w', 'GOPROXY=https://golang.flatt.tech']:
                print('configured detected Go')
            else:
                sys.exit(f'unexpected provisioning command: {args}')
        """)
        source = (ROOT / "jepsen/provision/base.sh").read_text()
        # Run only Go provisioning; sudo is intercepted before any host write.
        start = source.index("  GO_VERSION=")
        end = source.index('  if [ ! -f /home/vagrant/.ssh/id_rsa ]; then', start)
        result = subprocess.run(
            ["bash", "-euo", "pipefail", "-c", source[start:end]],
            cwd=self.work, env=self.env, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("configured detected Go", result.stdout)


if __name__ == "__main__":
    unittest.main()
