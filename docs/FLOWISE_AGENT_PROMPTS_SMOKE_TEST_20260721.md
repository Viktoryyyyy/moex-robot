# Flowise Agent Prompts Smoke Test

task_id: flowise-agent-prompts-smoke-20260721
purpose: validate Lead and Worker prompts, structured output, GitHub mutation, PR review and exact-head CI
production_change: false
server_apply: forbidden
merge: manual
correction_cycle: passed
expected_branch_reuse: true
expected_pr_reuse: true

Allowed scope:

- docs/FLOWISE_AGENT_PROMPTS_SMOKE_TEST_20260721.md

Forbidden scope:

- все остальные файлы;
- создание новой branch;
- создание нового Pull Request;
- изменение base branch;
- direct write в main;
- merge;
- server apply;
- закрытие PR;
- расширение scope.

## Task Contract

project: MOEX_Bot
task_id: flowise-agent-prompts-smoke-20260721
execution_id: flowise-agent-prompts-smoke-20260721-a3
repository: Viktoryyyyy/moex-robot
base_branch: main
working_branch: test/flowise-agent-prompts-smoke-20260721
pull_request: 270
exact_task: validate Flowise Lead and Worker mutation, correction, review and exact-head CI lifecycle
acceptance_criteria: one approved file changed; same branch and PR reused; review findings resolved; exact-head checks successful
required_result: structured Lead JSON with verified GitHub evidence
merge_mode: manual
server_apply: forbidden
