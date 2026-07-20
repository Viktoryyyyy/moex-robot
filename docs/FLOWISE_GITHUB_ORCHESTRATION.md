# Flowise GitHub Orchestration

## Назначение

Документ описывает текущую интеграцию GPT Action, proxy, Flowise, GitHub MCP и GitHub-репозитория.

## Схема вызова

`User / GPT Action`
→ `https://flowise-api.foods-tech.store/github-task`
→ proxy server
→ Flowise
→ Lead Agent `github-change-orchestrator`
→ `github_worker` и/или GitHub MCP
→ GitHub

## Серверы и пути

- Public endpoint: `https://flowise-api.foods-tech.store/github-task`
- Proxy server IP: `147.45.184.140`
- SSH reference: `trader@5768295-yd19673`
- Old Flowise server IP: `194.32.142.88`
- Proxy application path: `/opt/flowise_proxy.py`

Секреты, API keys, токены и пароли в этот документ не добавлять.

## Proxy behaviour

Proxy:
1. принимает `POST /github-task`;
2. проверяет наличие поля `question`;
3. передаёт задачу в Flowise;
4. фильтрует ответ;
5. возвращает наружу только:

```json
{
  "text": "..."
}
```

Внутренние runtime-поля Flowise наружу не передаются.

### Smoke test

Пустой запрос:

```bash
curl -i -X POST \
  https://flowise-api.foods-tech.store/github-task \
  -H 'Content-Type: application/json' \
  -d '{}'
```

Ожидаемый ответ:

```json
{
  "error": "Missing question"
}
```

## Порты

Точные внутренние порты proxy и Flowise в текущем handoff-контексте не подтверждены.

Не использовать предположения.

Статус:
- public HTTPS: доступен через стандартный TLS endpoint;
- internal proxy port: требуется проверить на proxy server;
- internal Flowise port: требуется проверить в конфигурации deployment;
- reverse proxy port mapping: требуется проверить на сервере.

Рекомендуемые команды проверки на сервере:

```bash
sudo ss -lntp
sudo systemctl status nginx
sudo nginx -T
sudo systemctl status flowise
ps aux | grep -i flowise
grep -R "listen\|proxy_pass" /etc/nginx /opt 2>/dev/null
```

Не публиковать вывод, содержащий credentials.

## Flowise entities

- Main flow: `github-change-orchestrator`
- Worker flow/agent: `github-worker`
- Lead tool name: `github_worker`
- Connection type: **Agent as Tool**

Текущая ответственность:
- Lead: анализ, контроль PR, review, checks, merge.
- Worker: чтение репозитория, ветка, изменения файлов, PR, исправления.
- Worker никогда не выполняет merge.

GitHub MCP может быть доступен:
- Lead — для read-only review, checks и merge;
- Worker — для чтения и изменения репозитория.

## Request contract

Рекомендуемый формат:

```text
Action: execute
Task ID: <unique-id>
Repository: <owner/repository>
Target branch: <branch>

Task:
<task description>

Acceptance criteria:
<criteria>

Merge mode: <manual|automatic>
```

Дополнительные поля:
- Allowed scope
- Constraints
- Working branch
- Pull request number
- Review comments

## Response contract

```json
{
  "taskId": "",
  "status": "",
  "summary": "",
  "branch": "",
  "pullRequestUrl": "",
  "checksStatus": "",
  "reviewStatus": "",
  "reviewComments": "[]",
  "mergeStatus": "",
  "errors": ""
}
```

### Допустимые статусы

`status`:
- `COMPLETED`
- `READY_FOR_MANUAL_MERGE`
- `MERGED`
- `BLOCKED`
- `FAILED`

`checksStatus`:
- `passed`
- `failed`
- `pending`
- `not_configured`

`reviewStatus`:
- `APPROVED`
- `CHANGES_REQUESTED`
- `NOT_PERFORMED`

`mergeStatus`:
- `merged`
- `not_merged`

## Критические правила

- Worker никогда не выполняет merge.
- Для любых изменений файлов Lead вызывает worker.
- После создания или обновления PR Lead обязан выполнить review.
- Если `pullRequestUrl` не пустой, `reviewStatus` не может быть `NOT_PERFORMED`.
- `READY_FOR_MANUAL_MERGE` и `MERGED` допустимы только при `reviewStatus=APPROVED`.
- Proxy возвращает только поле `text`.
- Checks нельзя выдумывать.
- Секреты нельзя сохранять в документации, prompts и логах.

## Troubleshooting

### GPT Actions timeout

GPT Actions имеет ограничение длительности запроса. Если пользователь получает timeout примерно через 45 секунд, это может быть лимит Action, даже если Flowise продолжает выполнение.

Проверить:
- длительность execution в Flowise;
- завершилась ли задача после разрыва Action;
- какой timeout настроен в proxy;
- какой timeout настроен на upstream.

### Upstream timeout

Признаки:
- proxy долго ждёт Flowise;
- в логах proxy появляется timeout;
- Flowise execution не завершён либо завис на tool call.

Проверить:
- proxy logs;
- Flowise execution trace;
- число повторных вызовов worker/MCP;
- циклы ожидания checks.

### Исторические ошибки

Ранее наблюдались:
- `fetch failed`;
- `SSE 405`;
- длительные зависания при попытках получить checks.

Текущий Agent as Tool transport устранил SSE-проблему.

### Где смотреть execution

В Flowise execution:
- найти узел Lead Agent;
- проверить tool calls;
- убедиться, что при изменении файлов вызван `github_worker`;
- проверить, был ли отдельный worker execution;
- проверить вызовы GitHub MCP;
- проверить итоговый structured output.

### Как понять, был ли вызван worker

Признаки:
- tool call с именем `github_worker`;
- отдельный execution worker;
- возвращённые worker branch/PR details.

Если видны только Lead Agent и GitHub MCP, Lead выполнил работу напрямую.
Для read-only задач это допустимо.
Для изменения файлов это нарушение архитектуры.

## Обязательный post-PR review

После каждого создания или обновления PR Lead обязан:

1. получить PR details;
2. получить changed files;
3. получить diff;
4. проверить acceptance criteria;
5. проверить scope;
6. выполнить code review;
7. получить checks;
8. определить blocking remarks;
9. при необходимости вернуть worker на исправление;
10. повторить review после обновления PR.

Если PR существует, но review не выполнен:

```text
status=BLOCKED
reviewStatus=NOT_PERFORMED
mergeStatus=not_merged
```

Недопустимо:

```text
pullRequestUrl != ""
reviewStatus=NOT_PERFORMED
status=READY_FOR_MANUAL_MERGE
```

## Review comments

Рекомендуемый формат:

```json
[
  {
    "file": "src/app.py",
    "line": 42,
    "priority": "blocking",
    "comment": "Описание проблемы"
  }
]
```

Если замечаний нет:

```json
[]
```

## Merge policy

### Manual

При `mergeMode=manual`:
- merge не выполнять;
- при успешном review:
  - `status=READY_FOR_MANUAL_MERGE`;
  - `reviewStatus=APPROVED`;
  - `mergeStatus=not_merged`.

### Automatic

При `mergeMode=automatic` merge разрешён только если:

1. PR существует;
2. review выполнен;
3. `reviewStatus=APPROVED`;
4. acceptance criteria выполнены;
5. `checksStatus=passed`;
6. blocking comments отсутствуют;
7. конфликтов нет;
8. пользователь не запретил merge.

После успешного merge:

```text
status=MERGED
mergeStatus=merged
```

Если merge невозможен:

```text
status=BLOCKED
mergeStatus=not_merged
```

## Ограничение циклов

Lead не должен бесконечно:
- вызывать worker;
- повторять checks;
- повторять review;
- ожидать изменение статуса.

Рекомендуется ограничить:
- worker fix cycles: 2;
- checks polling attempts: 2–3;
- общий execution time: меньше внешнего timeout с запасом.

При исчерпании лимита вернуть `BLOCKED` с фактической причиной.

## Выбор моделей

Lead и worker нужно тестировать отдельно.

### Lead

Требования:
- reasoning;
- orchestration;
- review;
- structured output;
- длинный контекст;
- управление циклами;
- merge decision.

### Worker

Требования:
- точные изменения кода;
- scope discipline;
- tool use;
- GitHub MCP;
- latency/cost balance.

### Минимальный A/B plan

Для каждой роли проверить 2–3 модели на одинаковых задачах:

1. read-only PR inspection;
2. docs-only PR;
3. blocking review → fix → repeated review;
4. automatic merge;
5. длинная задача с большим контекстом.

Метрики:
- success rate;
- latency;
- token usage;
- tool calls;
- retries;
- structured output errors;
- PR quality;
- review quality;
- scope violations;
- loop behaviour.

Результат зафиксировать в репозитории до смены production-моделей.

## Ближайшие действия

1. Проверить post-PR review на реальном PR.
2. Проверить цикл замечаний и исправлений.
3. Проверить automatic merge.
4. Зафиксировать источник `checksStatus`.
5. Провести A/B тест моделей Lead и worker.
6. Дополнить infrastructure documentation после фактической проверки портов и deployment.
