# Эксплуатация

## Запуск

Скопируйте `.env.example` в `.env`, задайте длинный случайный
`NEWSBOT_POSTGRES_PASSWORD` и остальные секреты, затем:

```bash
docker compose up -d --build
```

PostgreSQL доступен с хоста только через `127.0.0.1`. Контейнер приложения
сам применяет Alembic-миграции. Логи ротируются Docker.

## Резервное копирование

Сервис `backup` ежедневно создаёт custom-format dump в `./backups` и хранит
14 дней. Проверка восстановления в пустую БД:

```bash
createdb newsbot_restore_test
pg_restore --exit-on-error --clean --if-exists \
  --dbname=newsbot_restore_test backups/newsbot-YYYYMMDDTHHMMSSZ.dump
```

В production копии каталога `backups` должны уходить в отдельное
зашифрованное хранилище.

## Безопасное включение

Начинайте с `NEWSBOT_DRY_RUN=true`. Переключатель
`PUT /admin/publication/false` немедленно останавливает обработку outbox.
Telegram и MAX отдельно управляются через
`PUT /admin/platforms/{platform}/{enabled}`. Все изменения записываются в
`audit_logs`.

## Исправления

Неточный пост удаляется через `DELETE /publications/{id}`, событие можно
повторно обработать через `POST /events/{id}/reprocess`. Исправление должно
содержать фактическое уточнение и ссылки на первоисточники; исходные статьи,
claims, drafts и audit log сохраняются.
