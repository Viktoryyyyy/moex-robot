PROJECT=MOEX_Bot

# Воспроизведение исследования защитного буфера USDRUBF

[Выводы и памятка Прогнозисту](../../docs/research/usdrubf_protective_buffer_research_v1/README.md).
Изолированный исследовательский replay; не подключён к production.
Версии исходного расчёта: Python 3.12.14, pandas 3.0.1, numpy 2.3.5.
Читать `protocol.json` вместе с `protocol_amendment_01.json` и
`implementation_notes.txt`. Сетка и результаты при публикации не менялись.

Для повторного расчёта требуются снимки из исходного `reproducibility.zip`
у владельца исследования. Сырые рыночные данные не включены в публичный GitHub.
Проверить SHA256 архива из `publication_manifest.json`, затем хэши входных
файлов из `original_bundle_manifest.json` в каталоге документации.

В этом каталоге должны появиться:

```text
inputs/accepted_bars.csv
inputs/tradestats.csv
inputs/obstats.csv
inputs/history_all.json
inputs/source_manifest.json
inputs/minute_bars.json
inputs/minute_sources.json
inputs/security.json
inputs/candle_bounds.json
```

Установить `requirements.txt` в отдельное окружение и выполнить последовательно:

```sh
python prepare.py
python replay.py
python verify.py
python analyze.py
python supplement.py
python diagnostics.py
python build_report.py
```

Сеть для пересчёта не нужна. Скрипты создают производные `results/` и
`deliverable/`, не меняют `inputs/`. Эти каталоги исключены из Git.
Оригинальные скрипты выгрузки и инвентаризации сервера остаются в исходном
архиве; для чтения исследования и автономного replay они не требуются.
`build_report.py` собирает новый отчёт и локальный архив из доступных файлов;
он не воспроизводит байт-в-байт исходный ZIP с серверными вспомогательными
скриптами. Сравнивать нужно таблицы и пути исполнения, а не хэш нового ZIP.

До публикации исходный архив был распакован и независимо воспроизвёл
320 612 строк путей исполнения точно. Отдельно проверены 100 строк затрат,
фандинга и размера позиции. Исторические результаты проверок опубликованы
в документации; они относятся к исследованию, а не к прибыльности стратегии.
