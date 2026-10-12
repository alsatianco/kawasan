"""Exercise the unattended loop in isolated repositories with a fake Codex CLI."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2]
FAKE_CODEX = '''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys

if "--help" in sys.argv:
    print("--approve-for-me")
    sys.exit(0)
state = Path("AI-TEMP/calls")
count = int(state.read_text()) + 1 if state.exists() else 1
state.write_text(str(count))
Path(f"AI-TEMP/prompt-{count}.txt").write_text(sys.argv[-1])
mode = os.environ["FAKE_CODEX_MODE"]
status = "task_done"
commits = []
if mode in ("interrupted", "dirty_success", "always_fail", "blocked") and count == 1:
    Path("work.txt").write_text("unfinished change")
    Path("new.txt").write_text("unfinished new file")
    if mode == "interrupted":
        print(json.dumps({"type": "turn.failed", "error": {"message": "usage limit"}}))
        sys.exit(1)
    if mode == "always_fail":
        status = "failed"
    elif mode == "blocked":
        status = "blocked"
elif mode == "always_fail":
    status = "failed"
elif mode == "wrong_branch":
    subprocess.run(["git", "checkout", "-b", "unexpected"], check=True, capture_output=True)
elif mode == "all_done":
    status = "all_done"
else:
    subprocess.run(["git", "add", "work.txt", "new.txt"], check=True)
    subprocess.run(["git", "commit", "-qm", "Finish reviewed work"], check=True)
    commits = [subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()]
    with Path("AI-TEMP/improve-plan.md").open("a") as plan:
        plan.write("\\nReviewed and verified interrupted work.\\n")
result = Path(sys.argv[sys.argv.index("--output-last-message") + 1])
result.write_text(json.dumps({"status": status, "task": "fixture", "commits": commits, "note": "fixture"}))
'''


class AutoImproveTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="kawasan-auto-improve-")
        self.addCleanup(self.directory.cleanup)
        self.repo = Path(self.directory.name)
        shutil.copy2(SOURCE / "auto-improve.sh", self.repo)
        (self.repo / ".gitignore").write_text("AI-TEMP/\n")
        (self.repo / "work.txt").write_text("original")
        (self.repo / "AI-TEMP").mkdir()
        (self.repo / "AI-TEMP/improve-plan.md").write_text("Finish the fixture task.\n")
        binary = self.repo / "AI-TEMP/bin"
        binary.mkdir()
        (binary / "codex").write_text(FAKE_CODEX)
        (binary / "codex").chmod(0o755)
        self.env = dict(os.environ, PATH=f"{binary}:{os.environ['PATH']}",
                        AUTO_IMPROVE_AWAKE="1", MAX_RUNS="2", MAX_FAILS="2",
                        MAX_IDLE="2", RETRY_DELAY="0",
                        GIT_AUTHOR_NAME="Fixture", GIT_COMMITTER_NAME="Fixture",
                        GIT_AUTHOR_EMAIL="fixture@example.invalid",
                        GIT_COMMITTER_EMAIL="fixture@example.invalid")
        self.git("init", "-b", "main")
        self.git("add", ".")
        self.git("commit", "-qm", "Initial fixture")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, env=self.env, text=True).strip()

    def run_loop(self, mode, **settings):
        result = subprocess.run(["/bin/bash", "auto-improve.sh"], cwd=self.repo,
                                env=dict(self.env, FAKE_CODEX_MODE=mode, **settings),
                                text=True, capture_output=True, timeout=20)
        self.assertFalse((self.repo / "AI-TEMP/auto-improve/lock").exists())
        return result

    def prompt(self, run):
        return (self.repo / f"AI-TEMP/prompt-{run}.txt").read_text()

    def test_startup_reviews_staged_and_untracked_work(self):
        (self.repo / "work.txt").write_text("interrupted")
        self.git("add", "work.txt")
        (self.repo / "new.txt").write_text("new work")
        result = self.run_loop("recover", MAX_RUNS="1")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("This is a recovery session", self.prompt(1))
        self.assertEqual(self.git("status", "--porcelain"), "")
        run = (self.repo / "AI-TEMP/auto-improve/latest").resolve()
        self.assertIn("M  work.txt", (run / "status-before.txt").read_text())
        self.assertIn("?? new.txt", (run / "status-before.txt").read_text())
        self.assertIn("interrupted", (run / "tracked-before.patch").read_text())

    def test_interrupted_session_retries_with_recovery(self):
        result = self.run_loop("interrupted")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertNotIn("This is a recovery session", self.prompt(1))
        self.assertIn("This is a recovery session", self.prompt(2))
        self.assertEqual(self.git("status", "--porcelain"), "")

    def test_dirty_completion_is_rejected_and_recovered(self):
        result = self.run_loop("dirty_success")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("Rejecting completion claim", result.stdout)
        self.assertIn("This is a recovery session", self.prompt(2))
        self.assertEqual(self.git("status", "--porcelain"), "")

    def test_repeated_failure_preserves_work_and_respects_limit(self):
        result = self.run_loop("always_fail", MAX_RUNS="0")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("repeated session failures", result.stdout)
        self.assertEqual((self.repo / "AI-TEMP/calls").read_text(), "2")
        self.assertIn("M work.txt", self.git("status", "--porcelain"))
        self.assertTrue((self.repo / "new.txt").exists())
        self.assertEqual(self.git("stash", "list"), "")

    def test_blocked_preserves_work_without_retry(self):
        result = self.run_loop("blocked")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual((self.repo / "AI-TEMP/calls").read_text(), "1")
        self.assertTrue((self.repo / "new.txt").exists())

    def test_branch_guard_still_stops_before_and_after_sessions(self):
        self.git("checkout", "-b", "other")
        result = self.run_loop("recover")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertFalse((self.repo / "AI-TEMP/calls").exists())
        self.git("checkout", "main")
        result = self.run_loop("wrong_branch")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("expected main; found unexpected", result.stdout)

    def test_completion_still_requires_two_clean_audits(self):
        result = self.run_loop("all_done")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("independent completion audit", self.prompt(2))
        self.assertEqual(self.git("status", "--porcelain"), "")


if __name__ == "__main__":
    unittest.main()
