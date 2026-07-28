# Saratov NewsBot

Асинхронный агрегатор: RSS/HTML → проверяемые события → LLM draft → transactional
outbox → Telegram (Pyrofork) и MAX (Pyromax).

## Локальный запуск

```bash
cp .env.example .env
docker compose up -d postgres
uv sync --frozen --extra dev
uv run alembic upgrade head
uv run newsbot sources-sync
uv run newsbot serve
```

API: `http://127.0.0.1:8000/docs`. Один цикл: `uv run newsbot cycle`.
Отправка outbox: `uv run newsbot publish`. По умолчанию `NEWSBOT_DRY_RUN=true`.
Автономный последовательный worker: `uv run newsbot worker`.
Проверка готовности конфигурации без вывода секретов: `uv run newsbot doctor`.
Проверка всех источников: `uv run newsbot sources-check`.

Изменяющие состояние API-методы требуют
`Authorization: Bearer $NEWSBOT_MANAGEMENT_TOKEN`; без токена management API отключён.

Для реального режима заполните локальный `.env` и поставьте
`NEWSBOT_DRY_RUN=false`. Секреты не передавайте в чат и не коммитьте.

- Telegram: `NEWSBOT_TELEGRAM_API_ID`, `NEWSBOT_TELEGRAM_API_HASH`,
  `NEWSBOT_TELEGRAM_SESSION_STRING`, `NEWSBOT_TELEGRAM_CHAT_ID`.
- MAX: `NEWSBOT_MAX_TOKEN`, опционально `NEWSBOT_MAX_PASSWORD`,
  `NEWSBOT_MAX_CHAT_ID`. Аккаунт с этой сессией должен заранее вступить в
  приватный канал. Инвайты и ID каналов хранятся только локально.
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

Pyromax 0.7.x пока не экспортирует поддерживаемые edit/delete methods. Поэтому
обновление редактирует Telegram-пост, а в MAX создаёт новый пост с пометкой
«Обновление:». Прямые запросы в обход Pyromax не используются.

Неопределённые отправки Telegram сверяются по fingerprint командой
`uv run newsbot reconcile`. Pyromax 0.7.x не предоставляет чтение истории,
поэтому MAX разрешается явно:

```bash
uv run newsbot resolve-published JOB_UUID EXTERNAL_ID
uv run newsbot resolve-retry JOB_UUID
```

Пять реальных источников находятся в `config/sources.json`. Сначала обнаруживается
URL в RSS/списке, затем загружается и сохраняется полная страница статьи.

Перед живой публикацией оставьте `NEWSBOT_DRY_RUN=true`, запустите worker на 3–7 дней
и выгрузите последние 100 черновиков для ручной проверки:

```bash
uv run newsbot review-export --limit 100 --output dry-run-review.json
uv run newsbot dedupe-evaluate tests/fixtures/dedupe_pairs.json
```

`dry-run-review.json` содержит тексты, решения и ссылки на первоисточники; файл проверки
не следует коммитить.

Production-контейнер, kill switch, backup/restore и административные endpoints
описаны в `docs/operations.md`. Полный статус готовности ведётся в `ROADMAP.md`.

## Проверки

```bash
uv run ruff check .
uv run mypy
uv run pytest
```
