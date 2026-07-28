# Архитектура

## Решения

- Один асинхронный Python 3.12+ сервис: FastAPI для управления, CLI/worker для циклов.
- PostgreSQL и SQLAlchemy 2.x — источник истины. Готовый draft и outbox-задания
  создаются одной транзакцией.
- Источники хранятся в `sources`: RSS/HTML используют общую конфигурацию извлечения.
- RSS/страница списка используются только для обнаружения URL. Полная HTML-страница
  статьи сохраняется в `raw_articles`, очищенная версия — в `articles`.
- Идемпотентность и ревизии: уникально сочетание source + canonical URL + content hash;
  новый hash известного URL повторно проверяет прежнее событие.
- Кластеризация детерминированно учитывает 48-часовое окно, действие, адрес, числа
  и именованные сущности. Разные адреса не объединяются.
- LLM — OpenAI-совместимый клиент со строгими Pydantic JSON-схемами. Провайдер/model
  задаются окружением.
- `Publisher` изолирует Pyrofork и Pyromax. Dry-run реализует тот же интерфейс.
- Timeout после возможной отправки получает `UNCERTAIN`, а не слепой retry: у userbot API
  нет внешнего idempotency key. Telegram сверяется по fingerprint; MAX остаётся
  заблокированным до ручного решения, потому что Pyromax не читает историю.
- Publication job захватывается атомарным conditional update; retry использует backoff,
  jitter и dead-letter, а stale `SENDING` после рестарта требует reconciliation.
- PostgreSQL advisory lock и process-local fallback запрещают параллельные циклы.
- `runtime_control` хранит kill switch платформ, `audit_logs` — административные
  изменения. Состояние одинаково для API и отдельного worker-процесса.
- Источник получает HEALTHY/DEGRADED/UNAVAILABLE по загрузке полных страниц; circuit
  breaker изолирует постоянно падающий сайт.
- Management API закрыт bearer-токеном. Source fetch принимает только публичный HTTPS,
  повторно валидирует redirect и ограничивает размер ответа.
- Все даты UTC; локальное отображение использует `Europe/Saratov`.

## Поток первого среза

```text
RSS/HTML → discovery → article page → raw_articles → cleaned article revision
→ exact/context dedupe → event → claims/draft → validation → outbox → dry-run
```

## Осознанные ограничения

- embeddings и entity/action/location/time scorer для больших объёмов.
- Автоматический reconciliation MAX появится после поддержки истории в Pyromax;
  прямой HTTP в обход библиотеки запрещён.
- Автопубликация остаётся выключенной до временных этапов из `ROADMAP.md`:
  100–200 ручных проверок, 7 дней тестовых каналов и 14 дней стабильной работы.
