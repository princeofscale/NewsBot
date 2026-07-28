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
  нет внешнего idempotency key.
- Publication job захватывается атомарным conditional update; retry использует backoff,
  а stale `SENDING` после рестарта требует reconciliation.
- Management API закрыт bearer-токеном. Source fetch принимает только публичный HTTPS,
  повторно валидирует redirect и ограничивает размер ответа.
- Все даты UTC; локальное отображение использует `Europe/Saratov`.

## Поток первого среза

```text
RSS/HTML → discovery → article page → raw_articles → cleaned article revision
→ exact/context dedupe → event → claims/draft → validation → outbox → dry-run
```

## Следующее усиление

- embeddings и entity/action/location/time scorer для больших объёмов.
- reconciliation `UNCERTAIN` через историю платформ.
- production-метрики latency/error и alerting.
