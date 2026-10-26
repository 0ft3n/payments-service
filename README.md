# Payments Service

Микросервис асинхронной обработки платежей. Принимает запрос на оплату, обрабатывает его через эмулятор платёжного шлюза и сообщает клиенту результат через webhook.

**Стек:** FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker Compose.

## Быстрый старт

```bash
cp .env.example .env        # при желании поменяйте API_KEY
docker compose up -d --build
```

После запуска:

| Что | Адрес |
| --- | --- |
| API и Swagger UI | http://localhost:8000/docs |
| RabbitMQ Management (guest / guest) | http://localhost:15672 |
| Тестовый приёмник webhook | http://localhost:9000/webhooks |

Миграции применяются автоматически: сервис `migrations` выполняет `alembic upgrade head`, и только после этого стартуют `api`, `outbox-relay` и `consumer`.

## Примеры запросов

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key` (по умолчанию `change-me`).

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Оплата заказа #42",
        "metadata": {"order_id": 42, "customer": "ivanov"},
        "webhook_url": "http://webhook-receiver:9000/webhook"
      }'
```

```http
HTTP/1.1 202 Accepted

{"payment_id": "9b0c6c84-0f0a-4a43-9a4f-2c9f7d4c5e11", "status": "pending", "created_at": "2026-10-26T17:39:52.591020Z"}
```

Повторный запрос с тем же `Idempotency-Key` и тем же телом вернёт тот же платёж и заголовок `Idempotent-Replayed: true`. Тот же ключ с другим телом вернёт `409 Conflict`.

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/9b0c6c84-0f0a-4a43-9a4f-2c9f7d4c5e11 \
  -H "X-API-Key: change-me"
```

```json
{
  "payment_id": "9b0c6c84-0f0a-4a43-9a4f-2c9f7d4c5e11",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Оплата заказа #42",
  "metadata": {"order_id": 42, "customer": "ivanov"},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "http://webhook-receiver:9000/webhook",
  "created_at": "2026-10-26T17:39:52.591020Z",
  "processed_at": "2026-10-26T17:39:56.112409Z",
  "webhook_sent_at": "2026-10-26T17:39:56.130777Z"
}
```

Суммы передаются строкой, чтобы не терять точность decimal.

### Webhook

После обработки сервис отправляет `POST` на `webhook_url`:

```json
{
  "event": "payment.succeeded",
  "payment_id": "9b0c6c84-0f0a-4a43-9a4f-2c9f7d4c5e11",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Оплата заказа #42",
  "metadata": {"order_id": 42, "customer": "ivanov"},
  "created_at": "2026-10-26T17:39:52.591020+00:00",
  "processed_at": "2026-10-26T17:39:56.112409+00:00"
}
```

Доставка работает по схеме at-least-once, поэтому в заголовке `X-Webhook-Id` передаётся ключ `<payment_id>:<status>`, по которому получатель может отбросить дубли. Успешной доставкой считается любой ответ 2xx.

### Проверка retry и DLQ

В compose есть тестовый приёмник `webhook-receiver` с тремя адресами:

| `webhook_url` | Поведение |
| --- | --- |
| `http://webhook-receiver:9000/webhook` | всегда отвечает 200 |
| `http://webhook-receiver:9000/webhook/flaky` | первые две доставки отвечают 500, третья 200: платёж доходит с третьей попытки |
| `http://webhook-receiver:9000/webhook/fail` | всегда отвечает 500: после трёх попыток сообщение уходит в `payments.new.dlq` |

```bash
docker compose logs -f consumer       # попытки и задержки
curl http://localhost:9000/webhooks   # что получил приёмник
```

Сообщения в DLQ видно в RabbitMQ Management, очередь `payments.new.dlq`. В заголовках сообщения лежат номер попытки (`x-attempt`), последняя ошибка (`x-last-error`) и стандартный `x-death`.

## Архитектура

```
                ┌──────────────── одна транзакция ────────────────┐
 POST /payments │ INSERT payments ... ON CONFLICT (idempotency_key)│
 ───────────────►        INSERT outbox                            │
     202        └──────────────────────┬──────────────────────────┘
                                       │ outbox-relay: SELECT ... FOR UPDATE SKIP LOCKED
                                       ▼ publish (publisher confirms) → published_at = now()
                           exchange "payments" (direct)
                                       │ routing key payments.new
                                       ▼
                              queue "payments.new" ──────► consumer
                                  ▲          │               │ эмуляция шлюза 2–5 с, 90% успех
                    TTL истёк     │          │ reject        │ UPDATE status
                                  │          ▼               │ POST webhook
         queue "payments.new.retry.<N>ms"   exchange "payments.dlx"
                                  ▲          │
                                  │          ▼
                  ошибка, попытка < 3       queue "payments.new.dlq"
```

### Сервисы docker-compose

| Сервис | Назначение |
| --- | --- |
| `postgres`, `rabbitmq` | инфраструктура |
| `migrations` | одноразовый `alembic upgrade head` |
| `api` | FastAPI: приём и выдача платежей |
| `outbox-relay` | публикует события из таблицы `outbox` в RabbitMQ |
| `consumer` | FastStream: обработка платежа, webhook, retry, DLQ |
| `webhook-receiver` | тестовый приёмник webhook для демонстрации |

### Outbox pattern

Платёж и событие `payment.created` записываются в одной транзакции, поэтому событие не теряется и не появляется без платежа. API вообще не обращается к RabbitMQ: если брокер недоступен, платежи продолжают приниматься, а события уходят после его восстановления.

`outbox-relay` отдельным процессом забирает неопубликованные записи через `SELECT ... FOR UPDATE SKIP LOCKED` (несколько экземпляров relay не мешают друг другу), публикует их с publisher confirms и `mandatory`, после подтверждения брокера ставит `published_at`. Если публикация не удалась, в записи увеличивается `attempts` и сохраняется `last_error`, а событие уходит со следующей итерации. Если relay упадёт между публикацией и коммитом, событие будет отправлено повторно. Поэтому гарантия at-least-once, а consumer идемпотентен.

### Идемпотентность

- **API.** `idempotency_key` уникален в БД. Вставка идёт через `INSERT ... ON CONFLICT DO NOTHING`, так что даже одновременные дубли создают ровно один платёж и одно событие. Вместе с платежом хранится SHA-256 канонизированного тела запроса, и повтор ключа с другим телом получает `409`.
- **Consumer.** Сообщение может прийти повторно (retry, падение consumer, повтор от relay). Перед каждым шагом проверяется сохранённое состояние: обработанный платёж повторно через шлюз не проходит (статус меняется через `UPDATE ... WHERE status = 'pending'`), а уже доставленный webhook (`webhook_sent_at`) повторно не отправляется.

### Retry и DLQ

Любая ошибка обработки (недоступный webhook, ответ не 2xx, сбой БД) запускает повтор. Всего 3 попытки с экспоненциальной задержкой: после первой неудачи ждём 2 с, после второй 4 с (`RETRY_BASE_DELAY * 2^(n-1)`).

Задержка реализована средствами RabbitMQ, без `sleep` в consumer. Неудачное сообщение публикуется в очередь ожидания `payments.new.retry.<N>ms` без подписчиков, с `x-message-ttl`. По истечении TTL оно через dead-letter возвращается в exchange `payments` и снова попадает в `payments.new`. Номер попытки передаётся в заголовке `x-attempt`. Для каждой задержки своя очередь, поэтому сообщения с короткой задержкой не ждут за сообщениями с длинной.

После третьей неудачи consumer отклоняет сообщение (`reject` без requeue), и RabbitMQ по `x-dead-letter-exchange` очереди `payments.new` перекладывает его в `payments.dlx` → `payments.new.dlq`.

Отказ платёжного шлюза (10% случаев) ошибкой не считается: это бизнес-результат. Платёж получает статус `failed`, и клиент узнаёт об этом из webhook.

## Конфигурация

Все параметры задаются переменными окружения (см. `.env.example`):

| Переменная | По умолчанию | Описание |
| --- | --- | --- |
| `API_KEY` | `change-me` | статический ключ для `X-API-Key` |
| `DATABASE_URL` | собирается в compose | строка подключения SQLAlchemy (asyncpg) |
| `RABBITMQ_URL` | собирается в compose | строка подключения AMQP |
| `OUTBOX_POLL_INTERVAL` | `1.0` | пауза между опросами outbox, с |
| `OUTBOX_BATCH_SIZE` | `100` | событий за одну итерацию relay |
| `MAX_ATTEMPTS` | `3` | попыток обработки до отправки в DLQ |
| `RETRY_BASE_DELAY` | `2.0` | базовая задержка retry, с |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2.0` / `5.0` | время «обработки» в шлюзе, с |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `WEBHOOK_TIMEOUT` | `5.0` | таймаут запроса webhook, с |

## Тесты

Тесты используют настоящий PostgreSQL. Шлюз, webhook-эндпоинт и брокер подменяются (respx, `TestRabbitBroker`).

```bash
docker compose up -d postgres
docker compose --profile test run --rm tests
```

Покрыто: аутентификация, валидация, идемпотентность (включая конкурентные дубли и 409), outbox relay (пакеты, сбой брокера), обработка платежа (повторная доставка, сбой webhook) и логика retry/DLQ в consumer.

Локально без Docker:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
docker compose up -d postgres
pytest
```

## Структура проекта

```
app/
  api/            FastAPI: приложение, роуты, зависимости (X-API-Key, сессия БД)
  services/       бизнес-логика: создание платежа, обработка, шлюз, webhook
  outbox/         outbox relay (python -m app.outbox)
  consumer/       FastStream consumer
  messaging.py    топология RabbitMQ и контракт сообщения
  models.py       модели SQLAlchemy: payments, outbox
  schemas.py      схемы Pydantic
  config.py       настройки из окружения
migrations/       Alembic
tools/            тестовый приёмник webhook
tests/
```
