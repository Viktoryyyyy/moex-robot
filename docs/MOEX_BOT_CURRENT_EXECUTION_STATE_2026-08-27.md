# MOEX Bot — Current Execution State — 2026-08-27

status: current_execution_handoff
project: MOEX_Bot
repository: Viktoryyyyy/moex-robot
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
recorded_date: 2026-08-27

This document records dated execution metadata and verified server command forms. It is not a replacement for the management canon. GitHub remains Source of Truth and every SHA/state must be revalidated before a new mutation, merge or server apply.

## 1. Canonical server context

```text
HOME=/home/trader
repo_path=/home/trader/moex_bot/moex-robot
shell_path=~/moex_bot/moex-robot
command_prefix=cd ~/moex_bot && source venv/bin/activate && cd moex-robot
data_root=/home/trader/moex_bot/data
env_path=/home/trader/moex_bot/.env
```

Deprecated underscore repository-path variants are forbidden. The authoritative explicit deprecated-path list remains in `docs/MOEX_BOT_MANAGEMENT_CANON.md`; this operational handoff contains only active canonical hyphen-path commands.

Rules:

- use `moex-robot` with hyphen only;
- GitHub/repository is Source of Truth; server filesystem is Applied State only;
- never infer architecture or accepted state from server files alone;
- server commands are given one command at a time for phone paste;
- no heredoc for phone execution;
- do not invent server paths;
- server apply is allowed only for an exact merged GitHub SHA;
- stop on dirty working tree, non-`main` branch, origin/main SHA mismatch or non-fast-forward state;
- pilot/acceptance launch guards run in the foreground; only the `nohup` process may be backgrounded.

## 2. Canonical server apply command template

Before use, `<MERGED_SHA>` must be replaced with the exact SHA verified on GitHub `main`.

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && test -z "$(git status --porcelain)" && test "$(git branch --show-current)" = "main" && git fetch origin main && test "$(git rev-parse origin/main)" = "<MERGED_SHA>" && git merge --ff-only origin/main && test "$(git rev-parse HEAD)" = "<MERGED_SHA>" && echo PROJECT=MOEX_Bot ACTION=server_apply STATUS=APPLIED APPLIED_SHA=$(git rev-parse HEAD)
```

This command is the canonical apply pattern for current work. Do not replace it with a deprecated path or a command that pulls/merges without exact-SHA guards.

## 3. Stage status as of this record

```text
Stage 1: complete
Stage 2: complete; raw histories and content re-attestation accepted
Stage 3: complete and server-applied
Stage 4: complete; accepted/readiness/server-applied
Stage 5: complete; accepted/readiness/server-applied
Stage 6: folded into Stage 4; complete
Stage 7: complete; implementation, physical pilot, acceptance, readiness and server apply closed
Stage 8: WIP branch/open PR only; not accepted, not merged, not server-applied
Stage 9: pending
Stage 10: pending; scheduler must not be enabled before prerequisite readiness
Stage 11: pending Oil_RUB research
Stage 12: pending ML; PIT/no-lookahead only
```

## 4. Stage 7 repository metadata

### 4.1 Implementation PR

```text
PR: #385
branch: agent/step7-d1-w1-technical
merged implementation SHA: 21b57e54e993dd63f9f3a8b772bb39f39508db5e
server apply status: APPLIED
server applied SHA: 21b57e54e993dd63f9f3a8b772bb39f39508db5e
```

The implementation gate was merged only after exact-head CI PASS and fresh exact-head Codex review with no unresolved material findings.

### 4.2 Physical pilot

```text
run_id: step7_pilot_20260827_v1
status: pilot_passed
network_calls_used: false
latest_autodetect_used: false
continuous_series_used: false
run_artifacts_immutable: true
```

Accepted physical counts:

```text
USDRUBF D1 OHLCV: 1100
CNYRUBF D1 OHLCV: 1100
USDRUBF D1 technical: 1100
CNYRUBF D1 technical: 1100
USDRUBF completed W1 OHLCV: 224
CNYRUBF completed W1 OHLCV: 224
USDRUBF completed W1 technical: 224
CNYRUBF completed W1 technical: 224
outputs: 8
all output quality_status: pass
```

Native D1 `1100/1100` counts remain independently anchored by accepted Stage 2 history and must not be normalized to observed output.

### 4.3 Acceptance

```text
run_id: step7_pilot_20260827_v1
status: accepted
acceptance_contract_id: step7_rub_native_d1_w1_technical_acceptance.v1
accepted_pointer_count: 8
expected_pointer_count: 8
promotion_semantics: serialized_transactional_with_rollback
stage2_content_attestation_marker_sha256: 03ef2b6d554ce8857af614275ebe6ba699a47cd6e77f9507aa204c6424f789ff
```

Acceptance evidence confirmed:

- physical partition readback;
- frozen raw physical revalidation;
- current accepted raw scope match;
- independent D1/W1 oracle;
- independent technical oracle;
- physical row-count binding;
- single-descriptor output capture;
- output content SHA256 binding;
- output identity/SHA prewrite recheck;
- Stage 2 content-attestation lock held;
- Stage 7 publication lock held.

### 4.4 Readiness closure

```text
PR: #392
branch: agent/step7-readiness
readiness PR head before merge: e76ca8240501bacab9a015d320a754a97bdb3b38
readiness merge SHA: 19fa3525b79973da75e1111e496358b6c3f68d95
GitHub main after merge: 19fa3525b79973da75e1111e496358b6c3f68d95
server apply status: APPLIED
server applied SHA: 19fa3525b79973da75e1111e496358b6c3f68d95
```

Readiness flags after closure:

```text
implementation_ready: true
physical_pilot_passed: true
accepted_pointer_ready: true
scheduler_ready: false
research_ready: false
si_cr_continuous_ready: false
weekly_oi_ready: false
advanced_technical_policy_ready: false
```

The readiness contract test parses typed YAML values; substring checks are forbidden because `11000`, `2240` or `80` must not satisfy the `1100`, `224` or `8` invariants.

## 5. Stage 7 canonical server command forms

The commands below are pinned to the completed Stage 7 implementation SHA. They document the safe launch form for the completed run. The immutable run ID must not be reused. Any future pilot must use a new approved run ID and an exact approved implementation SHA.

### 5.1 Apply Stage 7 implementation SHA

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && test -z "$(git status --porcelain)" && test "$(git branch --show-current)" = "main" && git fetch origin main && test "$(git rev-parse origin/main)" = "21b57e54e993dd63f9f3a8b772bb39f39508db5e" && git merge --ff-only origin/main && test "$(git rev-parse HEAD)" = "21b57e54e993dd63f9f3a8b772bb39f39508db5e" && echo PROJECT=MOEX_Bot ACTION=server_apply STATUS=APPLIED APPLIED_SHA=$(git rev-parse HEAD)
```

### 5.2 Physical pilot start

All checkout/run guards execute in the foreground. Only the `nohup` runner is backgrounded.

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && test -z "$(git status --porcelain)" && test "$(git branch --show-current)" = "main" && test "$(git rev-parse HEAD)" = "21b57e54e993dd63f9f3a8b772bb39f39508db5e" && test ! -e /home/trader/moex_bot/data/runs/step7_rub_native_d1_w1/run_id=step7_pilot_20260827_v1 && test ! -e /home/trader/moex_bot/data/state/acceptance/step7_rub_native_d1_w1/run_id=step7_pilot_20260827_v1 && test ! -e /home/trader/moex_bot/step7_pilot_20260827_v1.log && (nohup env MOEX_DATA_ROOT=/home/trader/moex_bot/data PYTHONPATH=src python -m moex_data.step7_rub_native_d1_w1_pilot_runner --artifact-version step7_pilot_20260827_v1 --env-file /home/trader/moex_bot/.env > /home/trader/moex_bot/step7_pilot_20260827_v1.log 2>&1 < /dev/null & pid=$!; echo PROJECT=MOEX_Bot STATUS=STAGE7_V1_STARTED PID=$pid)
```

Pilot result log:

```bash
cat /home/trader/moex_bot/step7_pilot_20260827_v1.log
```

### 5.3 Acceptance start

Acceptance uses the same clean-tree, `main`, and exact implementation SHA guards. Only the `nohup` process is backgrounded.

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && test -z "$(git status --porcelain)" && test "$(git branch --show-current)" = "main" && test "$(git rev-parse HEAD)" = "21b57e54e993dd63f9f3a8b772bb39f39508db5e" && test ! -e /home/trader/moex_bot/data/state/acceptance/step7_rub_native_d1_w1/run_id=step7_pilot_20260827_v1/accepted_pointers.json && test ! -e /home/trader/moex_bot/step7_accept_20260827_v1.log && (nohup env MOEX_DATA_ROOT=/home/trader/moex_bot/data PYTHONPATH=src python -m moex_data.step7_rub_native_d1_w1_acceptance --run-id step7_pilot_20260827_v1 --repo-root . --env-file /home/trader/moex_bot/.env > /home/trader/moex_bot/step7_accept_20260827_v1.log 2>&1 < /dev/null & pid=$!; echo PROJECT=MOEX_Bot STATUS=STAGE7_ACCEPTANCE_STARTED PID=$pid)
```

Acceptance result log:

```bash
cat /home/trader/moex_bot/step7_accept_20260827_v1.log
```

### 5.4 Apply Stage 7 readiness closure

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && test -z "$(git status --porcelain)" && test "$(git branch --show-current)" = "main" && git fetch origin main && test "$(git rev-parse origin/main)" = "19fa3525b79973da75e1111e496358b6c3f68d95" && git merge --ff-only origin/main && test "$(git rev-parse HEAD)" = "19fa3525b79973da75e1111e496358b6c3f68d95" && echo PROJECT=MOEX_Bot ACTION=server_apply STATUS=APPLIED APPLIED_SHA=$(git rev-parse HEAD)
```

## 6. Stage 8 WIP metadata

Stage 8 is not part of accepted `main` at the time of this record.

```text
branch: agent/step8-position-risk-state
base/merge-base: 19fa3525b79973da75e1111e496358b6c3f68d95
current WIP head: e77d9ea6ae35b87d34d797f86e1022186e978f56
ahead_by: 4
behind_by: 0
PR: #393
PR state at record time: open
PR title: Implement Stage 8 bounded position risk state
merged: false
server_applied: false
accepted: false
```

WIP changed files:

```text
configs/datasets/step8_position_risk_state.v1.yaml
contracts/datasets/position_risk_state.v1.yaml
src/moex_data/step8_position_risk_state.py
tests/test_step8_position_risk_state.py
```

Current intended Stage 8 boundary:

- explicit manual/read-only broker snapshot input;
- deterministic position/risk-state validation and aggregation only;
- do not invent stop, max-loss, tranche, margin or sizing policy;
- do not recalculate futures P&L/scenario P&L until instrument payout/lot policy is explicitly approved;
- no trading orders, auto-sizing or recommendation generation;
- WIP branch must pass its own exact-head CI and fresh Codex review before any merge;
- no Stage 8 server apply until exact merged SHA exists.

## 7. Resume protocol

Before continuing work:

1. verify current GitHub `main` SHA;
2. verify whether `agent/step8-position-risk-state` still exists and its exact head;
3. verify PR #393 current state, base and exact head before using this dated record;
4. compare Stage 8 branch to current `main` before any further mutation;
5. preserve one task = one active route, one branch = one mutation owner, one PR = one task;
6. after any new commit, treat prior CI/review as stale;
7. merge only after exact-current-head CI PASS and fresh exact-current-head Codex review with no unresolved material findings;
8. server apply only exact merged SHA using the canonical guarded command template above.
