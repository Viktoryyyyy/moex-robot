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
