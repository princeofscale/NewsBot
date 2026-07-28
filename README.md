# Saratov NewsBot

Асинхронный агрегатор: RSS/HTML → проверяемые события → LLM draft → transactional
outbox → Telegram (Pyrofork) и MAX (Pyromax).

## Локальный запуск

```bash
cp .env.example .env
docker compose up -d
uv sync --extra dev
uv run alembic upgrade head
uv run newsbot serve
```

API: `http://127.0.0.1:8000/docs`. Один цикл: `uv run newsbot cycle`.
Отправка outbox: `uv run newsbot publish`. По умолчанию `NEWSBOT_DRY_RUN=true`.
Автономный последовательный worker: `uv run newsbot worker`.
Проверка готовности конфигурации без вывода секретов: `uv run newsbot doctor`.

Изменяющие состояние API-методы требуют
`Authorization: Bearer $NEWSBOT_MANAGEMENT_TOKEN`; без токена management API отключён.

Для реального режима заполните локальный `.env` и поставьте
`NEWSBOT_DRY_RUN=false`. Секреты не передавайте в чат и не коммитьте.

- Telegram: `NEWSBOT_TELEGRAM_API_ID`, `NEWSBOT_TELEGRAM_API_HASH`,
  `NEWSBOT_TELEGRAM_SESSION_STRING`. Целевая группа уже задана:
  `NEWSBOT_TELEGRAM_CHAT_ID=-1004308457179`.
- MAX: `NEWSBOT_MAX_TOKEN`, опционально `NEWSBOT_MAX_PASSWORD`,
  канал уже задан как `NEWSBOT_MAX_CHAT_ID=-77353283215547`. Аккаунт с этой
  сессией должен заранее вступить в приватный канал:
  <https://max.ru/join/Uz_qse_gn9RlnB_zhoS70zKK2lDQDcDOqKaX7tCx70Y>.
- CheapVibeCode: создайте ключ в <https://cheapvibecode.ru/portal/dashboard> и
  запишите его только в локальный `NEWSBOT_LLM_API_KEY`. Endpoint уже задан как
  `https://cheapvibecode.ru/v1`, модель — `deepseek-v4-flash`. Для переключения
  доступны `mimo-v2.5`, `mimo-v2.5-pro` и `deepseek-v4-pro`, если они разрешены
  созданному ключу.

Перед отключением dry-run пользовательские аккаунты Pyrofork/Pyromax должны
состоять в целевых чатах и иметь право отправлять сообщения. Затем:

```bash
uv run newsbot doctor
uv run newsbot cycle
uv run newsbot publish
```

Production-режим без LLM-ключа завершается ошибкой и никогда не публикует
черновик локального тестового генератора.

Pyromax 0.7.x пока не экспортирует поддерживаемые edit/delete methods; publish подключён,
а edit/delete fail closed вместо обхода библиотеки прямыми запросами.

## Проверки

```bash
uv run ruff check .
uv run mypy
uv run pytest
```
