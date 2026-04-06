# MOEX Bot — API база (Futures)

Базовые утилиты для получения котировок фьючерсов с Московской биржи
(через https://apim.moex.com) по ключевому слову — например "Si", "CNY", "RTS".

## Окружение
- Applied server repo root: `/home/trader/moex_bot/moex-robot`
- Applied venv: `/home/trader/moex_bot/venv`

## 1) fo_snapshot.py — текущие котировки
Получение актуального last, bid/ask, спреда, объёма.

```bash
FO_KEY=CNY python fo_snapshot.py
FO_KEY=Si  python fo_snapshot.py
```

## 2) fo_5m_day.py — 5-минутные свечи за день
По умолчанию — сегодня (МСК).

```bash
FO_KEY=CNY python fo_5m_day.py
FO_KEY=Si  FO_DAY=2025-10-23 python fo_5m_day.py
```

## 3) fo_5m_period.py — 5-минутные свечи за период
По умолчанию — последние 7 календарных дней.

```bash
FO_KEY=CNY python fo_5m_period.py
FO_KEY=Si  FO_FROM=2025-10-20 FO_TILL=2025-10-31 python fo_5m_period.py
```

Все утилиты используют lib_moex_api.py с функцией resolve_fut_by_key(key),
которая автоматически подбирает актуальный тикер по подстроке ("CNY", "Si", "RTS" и др.).
