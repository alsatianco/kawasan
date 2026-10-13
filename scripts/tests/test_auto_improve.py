"""Exercise the unattended loop in isolated repositories with a fake Codex CLI."""
import json
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
if mode == "stash_then_plan":
    subprocess.run(["git", "stash", "push", "--include-untracked", "-m", "Preserve unrelated work",
                    "--", "work.txt", "new.txt"], check=True, capture_output=True)
    Path("plan-task.txt").write_text("completed plan task")
    subprocess.run(["git", "add", "plan-task.txt"], check=True)
    subprocess.run(["git", "commit", "-qm", "Complete next plan task"], check=True)
    commits = [subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()]
    with Path("AI-TEMP/improve-plan.md").open("a") as plan:
        plan.write("\\nStashed unrelated work; completed the next task.\\n")
elif mode in ("interrupted", "dirty_success", "always_fail", "blocked") and count == 1:
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

FAKE_GH = '''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
with Path("AI-TEMP/gh-calls.jsonl").open("a") as calls:
    calls.write(json.dumps({"args": args, "codex_calls":
        Path("AI-TEMP/calls").read_text() if Path("AI-TEMP/calls").exists() else "0"}) + "\\n")
mode = os.environ.get("FAKE_GH_MODE", "success")
if args[0] == "api":
    if mode == "api_failure":
        print("GitHub unavailable", file=sys.stderr)
        sys.exit(1)
    if mode == "invalid_json":
        print("invalid json")
    else:
        print(os.environ.get("FAKE_GH_RUNS", '[{"workflow_runs": []}]'))
elif args[:2] == ["run", "view"]:
    if mode == "logs_failure" and "--log-failed" in args:
        sys.exit(1)
    print("test failure: expected 1, got 2" if "--log-failed" in args else
          json.dumps({"jobs": [{"name": "unit tests", "conclusion": "failure"}]}))
else:
    sys.exit(1)
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
        (binary / "gh").write_text(FAKE_GH)
        (binary / "gh").chmod(0o755)
        self.env = dict(os.environ, PATH=f"{binary}:{os.environ['PATH']}",
                        AUTO_IMPROVE_AWAKE="1", MAX_RUNS="2", MAX_FAILS="2",
                        MAX_IDLE="2", RETRY_DELAY="0",
                        GH_TOKEN="fixture-token", CI_REPO="alsatianco/peadb",
                        FAKE_GH_MODE="success", FAKE_GH_RUNS='[{"workflow_runs": []}]',
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

    def gh_calls(self):
        return [json.loads(line) for line in
                (self.repo / "AI-TEMP/gh-calls.jsonl").read_text().splitlines()]

    def ci_runs(self, *pages):
        return json.dumps([{"workflow_runs": runs} for runs in pages])

    def ci_run(self, number, conclusion="failure", workflow=1, branch="main"):
        return dict(id=number, run_number=number, workflow_id=workflow,
                    head_branch=branch, head_repository={"full_name": "alsatianco/peadb"},
                    conclusion=conclusion)

    def test_startup_reviews_staged_and_untracked_work(self):
        (self.repo / "work.txt").write_text("interrupted")
        self.git("add", "work.txt")
        (self.repo / "new.txt").write_text("new work")
        result = self.run_loop("recover", MAX_RUNS="1")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("This is a recovery session", self.prompt(1))
        self.assertIn("git stash push --include-untracked", self.prompt(1))
        self.assertIn("continue the plan in this same session", self.prompt(1))
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

    def test_stash_preserves_existing_work_and_plan_task_continues(self):
        (self.repo / "work.txt").write_text("unrelated change")
        self.git("add", "work.txt")
        (self.repo / "new.txt").write_text("unrelated new file")
        result = self.run_loop("stash_then_plan", MAX_RUNS="1")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual(self.git("status", "--porcelain"), "")
        self.assertIn("Preserve unrelated work", self.git("stash", "list"))
        self.assertEqual(self.git("show", "stash@{0}:work.txt"), "unrelated change")
        self.assertEqual(self.git("show", "stash@{0}^3:new.txt"), "unrelated new file")
        self.assertEqual(self.git("log", "-1", "--format=%s"), "Complete next plan task")

    def test_ci_is_checked_before_every_iteration_including_audits(self):
        result = self.run_loop("all_done")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = self.gh_calls()
        self.assertEqual([call["codex_calls"] for call in calls], ["0", "1"])
        self.assertTrue(all("--paginate" in call["args"] for call in calls))
        self.assertTrue(all("repos/alsatianco/peadb/actions/runs" in call["args"][-1]
                            for call in calls))
        self.assertIn("GitHub Actions preflight", self.prompt(2))

    def test_ci_collects_failures_across_pages_workflows_and_branches(self):
        runs = self.ci_runs(
            [self.ci_run(6, "success"), self.ci_run(5, "cancelled", workflow=2),
             self.ci_run(4, "failure", branch="feature")],
            [self.ci_run(3, "failure", workflow=2), self.ci_run(2, "failure")])
        result = self.run_loop("all_done", MAX_RUNS="1", FAKE_GH_RUNS=runs)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        calls = self.gh_calls()
        inspected = [call["args"][2] for call in calls if call["args"][0] == "run"]
        self.assertCountEqual(inspected, ["3", "3", "4", "4"])
        self.assertTrue(all(call["codex_calls"] == "0" for call in calls))
        run = (self.repo / "AI-TEMP/auto-improve/latest").resolve()
        self.assertIn("expected 1, got 2", (run / "ci-3.log").read_text())
        self.assertIn("commit each meaningful repair before continuing the plan", self.prompt(1))
        self.assertIn("already fix the same failure", self.prompt(1))

    def test_failed_ci_checks_stop_before_plan_work(self):
        for mode in ("api_failure", "invalid_json", "logs_failure"):
            with self.subTest(mode=mode):
                result = self.run_loop("all_done", FAKE_GH_MODE=mode,
                                       FAKE_GH_RUNS=self.ci_runs([self.ci_run(1)]))
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertFalse((self.repo / "AI-TEMP/calls").exists())

    def test_ci_repository_override(self):
        result = self.run_loop("all_done", MAX_RUNS="1", CI_REPO="alsatianco/kawasan")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("repos/alsatianco/kawasan/actions/runs", self.gh_calls()[0]["args"][-1])
        self.assertIn("Repository: alsatianco/kawasan", self.prompt(1))


if __name__ == "__main__":
    unittest.main()
