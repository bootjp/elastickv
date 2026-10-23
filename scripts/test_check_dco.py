"""Exercise the DCO checker against real, isolated Git repositories."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


CHECKER = Path(__file__).with_name("check-dco.py").resolve()
AUTHOR = "Contributor <contributor@example.com>"
SIGNOFF = "Signed-off-by: " + AUTHOR


class DCOTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name) / "repo"
        self.repo.mkdir()
        self.env = {
            **os.environ,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_AUTHOR_NAME": "Contributor",
            "GIT_AUTHOR_EMAIL": "contributor@example.com",
            "GIT_COMMITTER_NAME": "Maintainer",
            "GIT_COMMITTER_EMAIL": "maintainer@example.com",
        }
        self.git("init", "--initial-branch=main")
        # Old base history does not need a retroactive sign-off.
        self.base = self.commit("Unsigned existing history")

    def git(self, *args):
        return subprocess.run(
            ["git", *args], cwd=self.repo, env=self.env, check=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        ).stdout.strip()

    def commit(self, message):
        self.git("commit", "--allow-empty", "-m", message)
        return self.git("rev-parse", "HEAD")

    def check(self, expected, base=None, head="HEAD", cwd=None):
        result = subprocess.run(
            [sys.executable, str(CHECKER), base or self.base, head],
            cwd=cwd or self.repo, env=self.env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.assertEqual(expected, result.returncode, result.stdout + result.stderr)
        return result

    def test_signoff_variants(self):
        cases = [
            ("author", SIGNOFF, 0),
            ("case_insensitive_key", "signed-off-by: " + AUTHOR, 0),
            ("missing", "", 1),
            ("wrong_name", "Signed-off-by: Someone <contributor@example.com>", 1),
            ("wrong_email", "Signed-off-by: Contributor <someone@example.com>", 1),
            ("committer_only", "Signed-off-by: Maintainer <maintainer@example.com>", 1),
            ("malformed", "Signed-off-by: contributor@example.com", 1),
            ("prose", SIGNOFF + "\n\nThis is not a trailer block.", 1),
            ("multiple", "Signed-off-by: Someone <someone@example.com>\n" + SIGNOFF, 0),
        ]
        for name, trailer, expected in cases:
            with self.subTest(name=name):
                self.git("checkout", "-B", "case-" + name, self.base)
                self.commit("Contribution\n\n" + trailer)
                self.check(expected)

    def test_unicode_author(self):
        self.env["GIT_AUTHOR_NAME"] = "開発者"
        self.commit("Contribution\n\nSigned-off-by: 開発者 <contributor@example.com>")
        self.check(0)

    def test_bot_also_requires_matching_signoff(self):
        self.env["GIT_AUTHOR_NAME"] = "renovate[bot]"
        self.env["GIT_AUTHOR_EMAIL"] = "29139614+renovate[bot]@users.noreply.github.com"
        self.commit("Unsigned dependency update")
        self.check(1)
        identity = self.env["GIT_AUTHOR_NAME"] + " <" + self.env["GIT_AUTHOR_EMAIL"] + ">"
        self.git("commit", "--allow-empty", "--amend", "-m", "Dependency update\n\nSigned-off-by: " + identity)
        self.check(0)

    def test_checks_earlier_commits_and_all_failures(self):
        first = self.commit("Unsigned first contribution")
        second = self.commit("Unsigned second contribution")
        self.commit("Signed head\n\n" + SIGNOFF)
        result = self.check(1)
        self.assertIn(first, result.stderr)
        self.assertIn(second, result.stderr)
        self.assertIn("Checked 3 commit(s)", result.stdout)

    def test_diverged_base_excludes_its_unsigned_commits(self):
        self.git("checkout", "-b", "topic")
        head = self.commit("Contribution\n\n" + SIGNOFF)
        self.git("checkout", "main")
        base = self.commit("Unrelated unsigned base update")
        self.check(0, base=base, head=head)

    def test_merge_commit_requires_its_own_signoff(self):
        self.git("checkout", "-b", "topic")
        self.commit("Topic\n\n" + SIGNOFF)
        self.git("checkout", "main")
        self.commit("Main\n\n" + SIGNOFF)
        self.git("merge", "--no-ff", "topic", "-m", "Unsigned merge")
        merge = self.git("rev-parse", "HEAD")
        self.assertIn(merge, self.check(1).stderr)
        self.git("commit", "--amend", "-m", "Signed merge\n\n" + SIGNOFF)
        self.check(0)

    def test_no_mailmap_rewriting(self):
        self.commit("Contribution\n\n" + SIGNOFF)
        (self.repo / ".mailmap").write_text(
            "Other <other@example.com> " + AUTHOR + "\n", encoding="utf-8"
        )
        self.check(0)

    def test_invalid_email_cannot_certify(self):
        self.env["GIT_AUTHOR_EMAIL"] = "invalid"
        self.commit("Contribution\n\nSigned-off-by: Contributor <invalid>")
        self.check(1)

    def test_missing_history_fails(self):
        self.commit("Contribution\n\n" + SIGNOFF)
        self.check(1, base="f" * 40)

    def test_shallow_history_fails(self):
        self.commit("Contribution\n\n" + SIGNOFF)
        shallow = Path(self.temporary.name) / "shallow"
        self.git("clone", "--depth=1", self.repo.as_uri(), str(shallow))
        result = self.check(1, base="HEAD", cwd=shallow)
        self.assertIn("shallow repository", result.stderr)


if __name__ == "__main__":
    unittest.main()
