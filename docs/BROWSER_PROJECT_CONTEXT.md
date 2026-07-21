# MOEX Bot Browser Project Context

status: active_source
version: 1.0
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`

Используй этот текст как постоянный Project Context проекта MOEX Bot.

---

PROJECT=MOEX_Bot

Работай только в контексте проекта MOEX Bot. Не используй контекст, память, решения, пути или артефакты других проектов.

## Source of Truth

```text
GitHub repository = Source of Truth
Server filesystem = Applied State only
repository_full_name = Viktoryyyyy/moex-robot
```

Server filesystem не является архитектурным доказательством и не определяет repository state, branch ownership, accepted implementation или management authority.

## Canonical server context

```text
HOME=/home/trader
repo_path=/home/trader/moex_bot/moex-robot
shell_path=~/moex_bot/moex-robot
command_prefix=cd ~/moex_bot && source venv/bin/activate && cd moex-robot
```

Запрещённые устаревшие пути:

```text
/home/trader/moex_bot/moex_robot
~/moex_bot/moex_robot
cd ~/moex_bot/moex_robot && source venv/bin/activate
```

Не угадывай server paths. Для Flowise server tasks используй отдельную подтверждённую Flowise server instruction.

## Active execution routes

```text
browser_controlled_github_route
flowise_automated_github_route
```

Закрытый route:

```text
route_b_n8n_universal_role_runner
status: deprecated
new_tasks_allowed: false
```

Не создавай новые Route B / n8n задачи, ветки, PR или runtime действия.

## Context model

```text
Static Project Context
+ Persistent Role or Agent Context
+ Dynamic Task Contract
```

Task handoff содержит только динамические данные конкретной задачи. Не требуй повторения статического project context, role mandate, стандартного GitHub lifecycle или общего result schema.

## Task identity

```text
project: MOEX_Bot
root_task_id: stable parent ID
task_id: stable task ID
execution_id: unique execution attempt
attempt_no: incremented retry number
```

`task_id` сохраняется при retry и Browser ↔ Flowise transfer. `execution_id` меняется.

## Soft intake

Не блокируй задачу из-за отсутствия необязательных или восстанавливаемых данных.

Самостоятельно проверяй в GitHub:
- repository state;
- branch;
- PR;
- full head SHA;
- actual changed files;
- diff;
- reviews;
- checks;
- merge state.

Используй `BLOCKED` только если критическое значение необходимо для безопасного выполнения и не определяется ни из task context, ни из GitHub, ни из другого разрешённого источника.

## Ownership invariants

```text
one task = one active route
one branch = one active mutation owner
one PR = one task
one merge at a time
one server apply at a time
```

Read-only inspection допускается без mutation ownership. До mutation проверь route lock, branch owner, existing PR, exact head SHA и пересечение scope.

## GitHub lifecycle

Для mutation:

```text
verify GitHub state
→ use or create task branch
→ mutate only approved scope
→ create or update the same task PR
→ inspect exact changed files and diff
→ validate acceptance criteria
→ review
→ validate CI on exact latest head SHA
→ correct in the same branch and PR if required
→ merge only with explicit authority
```

Direct write в `main` запрещён.

## Review and CI

Green CI не означает merge readiness.

Если head SHA изменился:
- прошлый review устарел;
- прошлые checks не используются;
- merge delegation по старому SHA недействительно;
- review и CI проверяются повторно.

Не выдумывай branch, SHA, PR URL, changed files, checks, review, merge или server state.

## Merge

Default:

```text
merge_policy: manual
merge_delegated: false
```

Merge выполняется только при явном owner/PM authority и после проверки exact PR head, scope, acceptance criteria, review, exact-head CI и конфликтов.

Automatic merge delegation должно быть привязано к:

```text
task_id
repository
branch
pr_number
exact head_sha
merge_executor
```

## Server apply

Server apply является отдельным действием после merge.

Default:

```text
server_apply_allowed: false
server_apply_status: not_performed
```

Не выполняй server apply без отдельного явного разрешения и exact GitHub commit SHA.

## Route transfer and timeout

Browser ↔ Flowise transfer сохраняет task ID, approved scope, branch и PR. Меняются execution ID, attempt и route owner.

После Flowise timeout не повторяй mutation автоматически. Сначала выполни GitHub reconciliation и установи фактические branch, commits, PR и head SHA.

## Output rule

Каждый управленческий ответ начинается с:

```text
PROJECT=MOEX_Bot
```

Пиши профессионально, кратко и только подтверждённые данные. Не придумывай пути, состояние файлов или результаты выполнения.

Для команд на сервере давай одну порцию кода за раз, пригодную для вставки с телефона; не используй heredoc без прямой необходимости.

## Canonical management documents

```text
docs/MOEX_BOT_MANAGEMENT_CANON.md
docs/PM_L2_HANDOFF_PROMPT.md
docs/FLOWISE_GITHUB_ORCHESTRATION.md
docs/MOEX_BOT_CONTEXT_CONFIGURATION_SOURCES.md
```

При конфликте применяется `docs/MOEX_BOT_MANAGEMENT_CANON.md`, если owner не утвердил более новую версию.