#!/usr/bin/env bash
# Run one unattended Codex session per plan task, with a fresh completion audit.
# Compatible with macOS Bash 3.2. Requires codex, git and jq.
# Usage: ./auto-improve.sh [watch|--help]
# Optional environment: MAX_RUNS=0 MAX_FAILS=3 MAX_IDLE=2 RETRY_DELAY=60
# MAX_RUNS=0 means unlimited. Logs and plan snapshots stay in ignored AI-TEMP.
# Exit codes: 0 = verified complete; 1 = failed/stalled; 2 = blocked/config/limit.

set -euo pipefail

case "${1:-}" in
  --help|-h)
    sed -n '2,8p' "${BASH_SOURCE[0]}"
    exit 0
    ;;
  ''|watch) ;;
  *) printf 'Usage: %s [watch|--help]\n' "$0" >&2; exit 2 ;;
esac
[ "$#" -le 1 ] || { printf 'Too many arguments.\n' >&2; exit 2; }

# Long native and Docker gates must survive macOS idle sleep.
if [ "${1:-}" != watch ] && [ "$(uname -s)" = Darwin ] &&
   [ -z "${AUTO_IMPROVE_AWAKE:-}" ] && command -v caffeinate >/dev/null 2>&1; then
  export AUTO_IMPROVE_AWAKE=1
  exec caffeinate -is "$BASH" "${BASH_SOURCE[0]}" "$@"
fi

for tool in codex git jq; do
  command -v "$tool" >/dev/null 2>&1 || { printf '%s is required.\n' "$tool" >&2; exit 2; }
done
REPO=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
cd "$REPO"
PLAN=AI-TEMP/improve-plan.md
STATE_DIR=AI-TEMP/auto-improve
LOCK=$STATE_DIR/lock

if [ "${1:-}" = watch ]; then
  # Tail through the symlink so a new session is picked up automatically.
  tail -n +1 -F "$STATE_DIR/latest/stream.jsonl" | jq -Rr --unbuffered '
    fromjson? |
    if .type == "item.started" and .item.type == "command_execution" then
      "  > " + .item.command
    elif .type == "item.completed" and .item.type == "command_execution" then
      .item.aggregated_output // empty
    elif .type == "item.completed" and .item.type == "agent_message" then
      .item.text
    elif .type == "error" or .type == "turn.failed" then
      .message // .error.message // tostring
    else empty end'
  exit 0
fi

MAX_RUNS=${MAX_RUNS:-0}
MAX_FAILS=${MAX_FAILS:-3}
MAX_IDLE=${MAX_IDLE:-2}
RETRY_DELAY=${RETRY_DELAY:-60}
for setting in MAX_RUNS MAX_FAILS MAX_IDLE RETRY_DELAY; do
  value=${!setting}
  case "$value" in
    ''|*[!0-9]*) printf '%s must be a nonnegative integer.\n' "$setting" >&2; exit 2 ;;
  esac
  # Normalize leading zeros before Bash arithmetic.
  printf -v "$setting" '%d' "$((10#$value))"
done
[ "$MAX_FAILS" -gt 0 ] && [ "$MAX_IDLE" -gt 0 ] || {
  printf 'MAX_FAILS and MAX_IDLE must be positive.\n' >&2; exit 2;
}
[ -s "$PLAN" ] || { printf 'Missing or empty plan: %s\n' "$PLAN" >&2; exit 2; }
mkdir -p "$STATE_DIR"
git check-ignore -q "$STATE_DIR" || { printf '%s must be gitignored.\n' "$STATE_DIR" >&2; exit 2; }
if ! mkdir "$LOCK" 2>/dev/null; then
  printf 'Loop lock exists: %s/%s. Check its pid before removing a stale lock.\n' "$REPO" "$LOCK" >&2
  exit 2
fi
CHILD_PID=''
cleanup() {
  if [ -n "$CHILD_PID" ]; then
    kill -TERM "$CHILD_PID" 2>/dev/null || true
    wait "$CHILD_PID" 2>/dev/null || true
  fi
  rm -f "$LOCK/pid"
  rmdir "$LOCK"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "$$" > "$LOCK/pid"

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$STATE_DIR/loop.log"
}
check_repo() {
  local branch dirty
  branch=$(git symbolic-ref --quiet --short HEAD) || branch='(detached)'
  [ "$branch" = main ] || { log "STOP: expected main; found $branch."; return 1; }
  dirty=$(git status --porcelain --untracked-files=all)
  if [ -n "$dirty" ]; then
    log 'STOP: uncommitted changes need review before another session.'
    printf '%s\n' "$dirty" | tee -a "$STATE_DIR/loop.log"
    return 1
  fi
}
check_repo || exit 1
# Fail early on an older CLI rather than retrying unsupported permission flags.
cli_help=$(codex exec --help)
[[ "$cli_help" == *--approve-for-me* ]] || {
  log 'STOP: this Codex CLI does not support --approve-for-me; update it first.'
  exit 2
}

SCHEMA=$STATE_DIR/result-schema.json
cat > "$SCHEMA" <<'JSON'
{"type":"object","properties":{"status":{"type":"string","enum":["task_done","all_done","blocked","failed"]},"task":{"type":"string"},"commits":{"type":"array","items":{"type":"string"}},"note":{"type":"string"}},"required":["status","task","commits","note"],"additionalProperties":false}
JSON
PROMPT=$(cat <<'PROMPT'
please read AI-TEMP/improve-plan.md and do the next task. after done, pls update that file AI-TEMP/improve-plan.md so we know what to do next in the next session

You are running in an unattended auto-improve loop. The owner authorizes you to
make decisions autonomously from current requirements and the ultimate goal:
Kawasan should become a Kafka replacement. Kawasan has not been released; you
may change its architecture, APIs and storage formats without preserving older
Kawasan behavior. Kafka client/protocol interoperability remains essential.
This authorization supersedes older notes requiring owner approval for local
implementation or design choices. Record substantial decisions and reasons in
the plan. Nobody can answer questions in this session.

Rules:
1. Read the full plan and relevant references. Do the next unfinished task in
   dependency order, including implementation, relevant checks and documentation.
   Complete one coherent task, update the plan, then end this session. If an
   earlier run was interrupted, inspect its uncommitted work, history and plan,
   and finish that task first. Do not discard existing work.
2. Follow AGENTS.md and repository agreements. Commit after every meaningful
   change, with a concise one-line message. Work on main and preserve linear
   history. Never git push, publish externally, or modify remote settings or
   secrets. Do not rewrite history. Keep ignored AI-TEMP plans/logs local; never
   force-add them. Never display or commit credentials.
3. Make technical decisions yourself. Prioritize replication correctness,
   durable transactions, safe failover and real Kafka client compatibility.
   Update obsolete prerelease compatibility assumptions where appropriate.
   Preserve honest acceptance criteria: do not disable tests, weaken gates or
   call skipped, interrupted, unrun or failed checks successful to finish a task.
4. Run verification to completion before ending the session. Await any background
   jobs you started. If work fails, fix it autonomously where possible. If it
   cannot be finished, update the plan with recovery steps and report failed.
5. The plan has prose and tables, not necessarily checkboxes. Audit all remaining
   milestones, deferred tasks, referenced roadmap items and the definition of
   done. Never infer completion from an absence of unchecked boxes. If a task is
   blocked, record the exact reason and proceed with independent authorized work.
   Report blocked only when no such work remains. External publication and
   scheduled-nightly gates remain unfinished until genuinely satisfied; they
   must not be silently dropped or treated as complete.
6. Keep the plan self-contained: update current status, validation evidence,
   decisions, unfinished work and the exact next task. Do not claim completion
   based solely on historical evidence that does not cover the changed source.
7. Return the required JSON: task_done only after completing and committing the
   task and updating the plan; all_done only when EVERY plan task and acceptance
   gate is genuinely complete; blocked if only external/permission/time blockers
   remain (list them in note); failed otherwise. Include actual commit hashes.
   When the previous session reported all_done, independently audit that claim
   against the source and plan; resume work if anything remains.
PROMPT
)

runs=0
fails=0
idle=0
done_streak=0
log 'Starting: model=gpt-6.1-sol effort=high permissions=automatic review.'
log "Watch in another terminal: $REPO/auto-improve.sh watch"
while :; do
  if [ "$MAX_RUNS" -gt 0 ] && [ "$runs" -ge "$MAX_RUNS" ]; then
    log "STOP: reached MAX_RUNS=$MAX_RUNS; completion has not been verified."
    exit 2
  fi
  check_repo || exit 1
  runs=$((runs + 1))
  RUN_DIR=$(mktemp -d "$STATE_DIR/run-$(date '+%Y%m%d-%H%M%S')-$runs.XXXXXX")
  ln -sfn "${RUN_DIR##*/}" "$STATE_DIR/latest"
  cp "$PLAN" "$RUN_DIR/plan-before.md"
  head_before=$(git rev-parse HEAD)
  log "Session $runs: $RUN_DIR"
  session_prompt=$PROMPT
  if [ "$done_streak" -gt 0 ]; then
    session_prompt="$PROMPT

The previous session reported all_done. This is the independent completion audit."
  fi
  codex exec --model gpt-6.1-sol -c 'model_reasoning_effort="high"' \
    --approve-for-me --cd "$REPO" --json --color never \
    --output-schema "$SCHEMA" --output-last-message "$RUN_DIR/result.json" \
    "$session_prompt" </dev/null > "$RUN_DIR/stream.jsonl" 2> "$RUN_DIR/stderr.txt" &
  CHILD_PID=$!
  rc=0
  wait "$CHILD_PID" || rc=$?
  CHILD_PID=''
  [ -s "$PLAN" ] || { log 'STOP: session removed or emptied the plan.'; exit 1; }
  cp "$PLAN" "$RUN_DIR/plan-after.md"
  head_after=$(git rev-parse HEAD)
  git merge-base --is-ancestor "$head_before" "$head_after" || {
    log 'STOP: session rewrote history.'; exit 1;
  }
  git log --oneline "$head_before..$head_after" > "$RUN_DIR/commits.txt"
  new_commits=$(git rev-list --count "$head_before..$head_after")
  status=failed
  if [ "$rc" -eq 0 ] && jq -e '
    type == "object" and
    (.status == "task_done" or .status == "all_done" or .status == "blocked" or .status == "failed") and
    (.task | type == "string") and (.commits | type == "array") and
    (.commits | all(.[]; type == "string")) and (.note | type == "string")
  ' "$RUN_DIR/result.json" >/dev/null 2>&1; then
    status=$(jq -r '.status' "$RUN_DIR/result.json")
    log "$(jq -r '.task + ": " + .note | gsub("[\n\r\t]"; " ")' "$RUN_DIR/result.json")"
  else
    log "Session failed or returned an invalid result (exit $rc); see $RUN_DIR."
  fi
  log "Session $runs: status=$status, new commits=$new_commits"
  check_repo || exit 1
  case "$status" in
    task_done)
      fails=0
      done_streak=0
      if [ "$new_commits" -eq 0 ] || cmp -s "$RUN_DIR/plan-before.md" "$PLAN"; then
        idle=$((idle + 1))
        log 'No verified progress: task_done requires new commits and an updated plan.'
        [ "$idle" -lt "$MAX_IDLE" ] || { log 'STOP: repeated lack of progress.'; exit 1; }
      else
        idle=0
      fi
      ;;
    all_done)
      fails=0
      idle=0
      done_streak=$((done_streak + 1))
      [ "$done_streak" -lt 2 ] || { log 'DONE: two fresh sessions verified every plan task is complete.'; exit 0; }
      log 'Starting a fresh session to independently verify completion.'
      ;;
    blocked)
      log 'STOP: unfinished tasks require external action or time; see result.json and the updated plan.'
      exit 2
      ;;
    failed)
      done_streak=0
      fails=$((fails + 1))
      [ "$fails" -lt "$MAX_FAILS" ] || { log 'STOP: repeated session failures.'; exit 1; }
      log "Retrying in $RETRY_DELAY seconds; the next session will recover unfinished work."
      sleep "$RETRY_DELAY" &
      CHILD_PID=$!
      wait "$CHILD_PID"
      CHILD_PID=''
      ;;
  esac
done
