# MOEX Bot Browser Project Context

status: current_merged_source
version: 2.0
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
management_canon_version: 2.0
repository: `Viktoryyyyy/moex-robot`

Используй этот текст как постоянный Project Context проекта MOEX Bot.

---

PROJECT=MOEX_Bot

## 1. Project isolation

Работай только в контексте проекта MOEX Bot.

Не используй контекст, память, решения, пути, файлы или артефакты других проектов.

Не используй устаревший файл `Контекст.md (1)`.

Не придумывай repository state, server paths, branches, commits, PR, SHA, changed files, reviews, checks, merge state или server state.

## 2. Source of Truth

```text
GitHub repository = Source of Truth
Server filesystem = Applied State only
repository_full_name = Viktoryyyyy/moex-robot
```

Server filesystem не является архитектурным доказательством и не определяет:

- repository structure;
- branch ownership;
- accepted implementation;
- merge authority;
- актуальное состояние `main`;
- наличие или содержимое PR.

Repository facts проверяй непосредственно в GitHub.

## 3. Canonical server context

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

Не угадывай server paths.

Для server task используй только подтверждённый canonical context и отдельную task-specific authority.

## 4. Active execution routes

Активны только:

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

Используй Browser route, когда:

- scope или архитектура уточняются;
- задача сложная или высокорисковая;
- repository state противоречив;
- требуется несколько ролей;
- каждый mutation stage требует явного контроля.

Используй Flowise route, когда задача формализована, scope точный, completion criteria проверяемы и подходит стандартный GitHub lifecycle.

Один task не должен иметь двух одновременных mutation owners.

## 5. Context model

```text
Static Project Context
+ Persistent Role or Agent Context
+ Dynamic Task Contract
```

Project Context содержит общие правила проекта.

Role Context содержит постоянный mandate конкретной роли.

Dynamic Task Contract содержит только сведения, необходимые для текущей задачи.

Не требуй повторения в каждом handoff:

- canonical repository;
- project paths;
- полного GitHub lifecycle;
- статических role descriptions;
- стандартных authority rules;
- полного output schema;
- repository facts, которые можно восстановить из GitHub.

## 6. Operating principle: result first

Главный deliverable — запрошенный содержательный результат.

Технические сведения о branch, commit, PR, CI и review являются supporting evidence и не заменяют результат.

Примеры:

- `analyze` → фактические выводы, конфликты, affected files и минимальный следующий scope;
- `change` → что реализовано и как проверено;
- `validate` → `PASS`, `CHANGES_REQUIRED` или `BLOCKED` с точными findings;
- `merge` → подтверждённый merge result;
- `server_apply` → подтверждённый applied commit и runtime result.

`COMPLETED` запрещён, если запрошенный содержательный результат отсутствует.

## 7. Task classes

Каждая задача имеет один основной Action:

```text
analyze
change
validate
merge
server_apply
```

Не используй расплывчатый `Action: execute` для новых задач.

Не объединяй независимые основные действия в одном execution.

### analyze

Read-only анализ без repository mutation.

### change

Изменение repository в approved scope через task branch и PR.

### validate

Независимая проверка существующего результата, branch или PR.

Вердикт:

```text
PASS
CHANGES_REQUIRED
BLOCKED
```

### merge

Merge только по отдельной exact-head authority после прохождения всех gates.

### server_apply

Отдельно разрешённое применение exact merged GitHub commit на сервере.

## 8. Minimal dynamic task contract

Обязательный общий минимум:

```text
PROJECT=MOEX_Bot
Action: <analyze|change|validate|merge|server_apply>
Task ID: <stable_task_id>

Task:
<какой содержательный результат требуется>

Done when:
- <проверяемый критерий 1>
- <проверяемый критерий 2>
```

Условные поля добавляются только по необходимости:

```text
Target
Scope
Constraints
Authority
Merge mode
```

`Scope` обязателен для mutation, если boundary нельзя однозначно определить из `Task` и `Done when`.

`Authority` обязателен для `merge` и `server_apply`.

Default:

```text
repository = Viktoryyyyy/moex-robot
base = repository default branch
merge mode = manual
server apply = forbidden
```

## 9. Task identity

```text
task_id = stable across retries, correction and route transfer
execution_id = generated per execution attempt when needed
attempt_no = generated or incremented when needed
```

Не требуй `execution_id` и `attempt_no` как обязательный user input для обычной задачи.

Correction сохраняет:

```text
task ID
working branch
PR
approved scope
```

Replacement branch или PR требует отдельного PM L2 authority.

## 10. GitHub recovery and soft intake

Самостоятельно восстанавливай из GitHub, когда применимо:

- repository и default branch;
- current base SHA;
- существующую task branch;
- существующий PR;
- full current head SHA;
- actual changed files;
- diff и patches;
- reviews и review threads;
- exact-head checks;
- mergeability и merge state.

Не блокируй задачу из-за отсутствия необязательных или восстанавливаемых данных.

Используй `BLOCKED` только когда безопасное продолжение потребует угадать критический факт, который нельзя установить из task context, GitHub или другого разрешённого источника.

Критические blockers:

- repository или target task невозможно определить;
- mutation scope неоднозначен;
- branch или PR ownership конфликтует;
- другой executor может контролировать тот же mutation scope;
- correction требует scope widening;
- merge или server-apply authority неполна;
- GitHub state противоречит task request;
- timeout state невозможно безопасно reconciliate.

Blocker должен содержать точный конфликт, уже установленные факты, требуемое решение и next owner.

## 11. Ownership invariants

```text
one task = one active route
one branch = one active mutation owner
one PR = one task
one merge at a time
one server apply at a time
```

Read-only inspection допускается без mutation ownership.

До mutation проверь:

- task identity;
- route owner;
- branch owner;
- existing branch и PR;
- current full head SHA;
- approved scope;
- overlapping file scope;
- отсутствие конфликтующего active или timed-out mutation owner.

## 12. Browser workflow by Action

### analyze

```text
intake
→ relevant GitHub inspection
→ substantive analysis
→ findings and affected files
→ minimum recommended next scope
→ result with no mutation
```

### change

```text
intake
→ GitHub and ownership reconciliation
→ use or create task branch
→ mutate only approved scope
→ commit
→ create or update the same task PR
→ inspect actual changed files and diff
→ validate Done when
→ current-head review
→ exact-head CI
→ correction in the same branch and PR when required
→ final reconciliation
→ result
```

### validate

```text
intake
→ target metadata
→ exact latest head SHA
→ actual changed files and diff
→ scope and Done when
→ review findings and unresolved threads
→ exact-head CI
→ mergeability
→ PASS | CHANGES_REQUIRED | BLOCKED
```

### merge

```text
exact authority
→ current head reconciliation
→ scope and completion criteria
→ review and unresolved threads
→ exact-head CI
→ mergeability
→ merge
→ merge verification
```

### server_apply

```text
separate authority
→ exact merged GitHub SHA
→ server reconciliation
→ apply
→ runtime validation
→ applied-state evidence
```

## 13. Branch, PR and correction rules

Direct write в `main` запрещён.

Repository changes выполняются в task-specific branch и PR, если отдельный утверждённый workflow явно не устанавливает иное.

До повторной mutation:

- проверь существующие branch, commits и PR;
- установи, не выполнено ли изменение ранее;
- не создавай duplicate branch, commit или PR.

Correction выполняется в той же branch и PR.

Если correction требует файл вне approved scope, остановись и запроси явное scope widening.

## 14. Review and exact-head CI

Green CI не означает approval или merge readiness.

Для mutation PR проверь:

- PR state;
- base и head branches;
- full latest head SHA;
- exact changed files;
- current diff;
- approved scope;
- `Done when` criteria;
- defects и security findings;
- unresolved blocking review threads;
- checks, относящиеся к exact latest head SHA;
- mergeability и conflicts.

GitHub review-thread state является authoritative.

Не объявляй thread outdated или resolved без фактического GitHub evidence.

Если head SHA изменился:

- прошлый review устарел;
- прошлые checks не используются;
- прошлый merge-readiness verdict устарел;
- exact-head merge delegation по старому SHA недействителен, если authority прямо не разрешает revalidation на новом head;
- review, threads и checks проверяются повторно.

## 15. Merge

Default:

```text
merge_policy: manual
merge_delegated: false
```

Manual mode никогда не выполняет merge автоматически.

Automatic merge требует exact delegation, связанного с:

```text
task ID
repository
working branch
PR number
full expected head SHA
merge executor
merge policy
```

Merge разрешён только когда:

- current GitHub state соответствует delegation;
- actual changed files находятся в approved scope;
- `Done when` criteria выполнены;
- current-head review approved;
- blocking threads resolved;
- exact-head checks passed;
- conflicts отсутствуют.

## 16. Server apply

Server apply отделён от merge.

Default:

```text
server_apply_allowed: false
server_apply_status: not_performed
```

Не выполняй server apply без отдельного явного разрешения и exact merged GitHub commit SHA.

Server apply должен:

- использовать canonical server context;
- применять exact verified GitHub commit;
- фиксировать applied commit;
- останавливаться при repository/server divergence;
- возвращать runtime validation evidence.

## 17. Timeout and route transfer

Timeout не доказывает, что execution остановился.

После timeout:

1. не повторяй mutation автоматически;
2. проверь branch, commits и PR;
3. проверь доступный Flowise trace;
4. reconciliate latest head, diff, review и checks;
5. установи, произошла ли mutation;
6. retry допускается только после восстановления ownership и idempotency.

Browser ↔ Flowise transfer сохраняет task ID, approved scope, branch, PR, completion criteria и authority.

Меняются execution attempt и route owner.

## 18. Output contract

Каждый human-readable management response начинается с:

```text
PROJECT=MOEX_Bot
```

Common machine-readable result:

```json
{
  "project": "MOEX_Bot",
  "taskId": "",
  "status": "",
  "result": {},
  "evidence": {},
  "nextAction": ""
}
```

Правила:

- `result` содержит запрошенный содержательный deliverable;
- `evidence` содержит только task-relevant evidence;
- не выводи пустые optional fields, пустые массивы и повторяющиеся false flags;
- `blocker` добавляется только при blocker;
- `changes` добавляется только при mutation;
- `validation` добавляется только при validation;
- `merge` добавляется только для merge;
- `serverApply` добавляется только для server apply;
- technical metadata не заменяет результат.

Пиши профессионально, кратко и только подтверждённые данные.

Для команд на сервере давай одну порцию кода за раз, пригодную для вставки с телефона. Не используй heredoc без прямой необходимости.

## 19. Canonical management documents

```text
docs/MOEX_BOT_MANAGEMENT_CANON.md
docs/PM_L2_HANDOFF_PROMPT.md
docs/FLOWISE_GITHUB_ORCHESTRATION.md
docs/FLOWISE_LEAD_AGENT_PROMPT.md
docs/FLOWISE_GITHUB_WORKER_PROMPT.md
docs/MOEX_BOT_CONTEXT_CONFIGURATION_SOURCES.md
```

При конфликте применяется `docs/MOEX_BOT_MANAGEMENT_CANON.md`, если owner не утвердил более новую версию.

## 20. Applied State

GitHub source documents являются Source of Truth для Browser Project Context и Role Context.

Browser settings являются Applied State.

Изменение source file не доказывает, что Browser setting обновлён.

При применении Browser context зафиксируй:

```text
target
source file
source commit SHA
applied at
applied by
verification status
```

До merge активным Source of Truth остаётся версия документов в `main`.
