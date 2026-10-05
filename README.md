# payment-service

Асинхронный микросервис приёма платежей: API принимает заявку, отдаёт её в RabbitMQ, отдельный
воркер проводит платёж через внешний шлюз и доставляет результат клиенту на webhook.

Ключевые свойства: идемпотентность на входе, transactional outbox против потери событий,
явные retry-очереди с TTL и DLQ, ручной ack, HMAC-подпись webhook, структурированные логи.

## Возможности

| Требование | Реализация |
| --- | --- |
| Приём платежа | `POST /api/v1/payments` → `202 Accepted`, ответ содержит только `payment_id`, `status`, `created_at` |
| Идемпотентность | Уникальный индекс на `idempotency_key`; повтор возвращает тот же `payment_id` с `Idempotency-Replayed: true`, другое тело — `409` |
| Асинхронная обработка | Transactional outbox + RabbitMQ, обработка вне HTTP-запроса |
| Статусы | Только `pending` → `succeeded` / `failed`, переходы защищены `UPDATE ... WHERE status = 'pending'` |
| Повторы | N retry-очередей с TTL (5 с, 15 с), экспоненциальная задержка, DLQ после исчерпания попыток |
| Webhook | До 3 попыток с exponential backoff, подпись `X-Signature: sha256=<hex>` |
| Наблюдаемость | JSON-логи, `/health/live`, `/health/ready`, колонки `webhook_attempts` / `webhook_last_error` |

## Стек

- Python 3.12, FastAPI, Pydantic v2, pydantic-settings
- SQLAlchemy 2.0 (async), asyncpg, Alembic, PostgreSQL 16
- FastStream 0.7.7 + aio-pika, RabbitMQ 3.13
- pytest, pytest-asyncio, ruff, mypy
- Docker, Docker Compose

## Архитектура

```mermaid
flowchart LR
    client[Клиент] -->|POST /api/v1/payments| api[API · FastAPI]
    api -->|одна транзакция| pg[(PostgreSQL<br/>payments + outbox_events)]
    api -->|relay: SELECT FOR UPDATE SKIP LOCKED| rmq{{RabbitMQ}}
    rmq --> consumer[Consumer · FastStream]
    consumer --> gateway[Payment Gateway]
    consumer -->|подписанный POST| hook[Клиентский webhook]
    consumer --> pg
    rmq --> retry1[payments.retry.1 · TTL]
    rmq --> retry2[payments.retry.2 · TTL]
    retry1 --> rmq
    retry2 --> rmq
    rmq --> dlq[payments.dlq]
```

Путь одного платежа:

1. `POST /api/v1/payments` валидирует тело, проверяет `X-API-Key` и `Idempotency-Key`.
2. В одной транзакции создаются строка `payments` и событие `outbox_events` со статусом `pending`.
   Если `idempotency_key` уже существует — транзакция откатывается, клиенту возвращается
   существующий платёж (или `409`, если тело отличается).
3. Клиенту сразу отвечают `202`. Никакой внешней обработки в HTTP-запросе нет.
4. Relay в lifespan API выбирает пачку событий `FOR UPDATE SKIP LOCKED`, публикует их в
   RabbitMQ и переводит в `published`. При ошибке брокера событие возвращается в `pending`
   с накопленным `attempts` и `available_at` (экспоненциальная задержка), после
   `OUTBOX_MAX_ATTEMPTS` — `dead`.
5. Consumer читает `payments.new`, вручную acкает сообщение только после успешной обработки,
   повторно инкрементит `processing_attempts` и вызывает шлюз.
6. Шлюз либо возвращает ссылку на операцию (`succeeded`), либо отказ (`failed`), либо
   временную ошибку — тогда сообщение уходит в retry-очередь, а после последней попытки в DLQ.
7. После терминального статуса отправляется webhook. Его недоставка не «откатывает» результат:
   платёж уже терминальный, ошибка фиксируется в `webhook_last_error`.

## Быстрый старт

```bash
cp .env.example .env
docker compose up -d --build
```

Compose поднимает `postgres`, `rabbitmq`, прогоняет миграции (`migrate`), затем `api` и
`consumer`. Демо-приёмник webhook включается отдельно:

```bash
docker compose --profile demo up -d --build
```

Готовый сценарий с демо-приёмником:

```bash
curl -sS -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-dev-api-key' \
  -H 'Idempotency-Key: order-A-10293' \
  -H 'Content-Type: application/json' \
  -d '{
        "amount": "1490.50",
        "currency": "RUB",
        "description": "Order #A-10293",
        "metadata": {"order_id": "A-10293"},
        "webhook_url": "http://localhost:8080/hooks/payments"
      }'
```

Через 2–5 секунд платёж переходит в `succeeded` (или `failed`), событие приходит на
`http://localhost:8080/hooks/payments`, а состояние видно так:

```bash
curl -sS http://localhost:8000/api/v1/payments/<payment_id> \
  -H 'X-API-Key: local-dev-api-key'
docker compose logs -f consumer
```

Полезные адреса:

| Что | Адрес |
| --- | --- |
| Swagger UI | http://localhost:8000/docs |
| Health | http://localhost:8000/health/live, http://localhost:8000/health/ready |
| RabbitMQ Management | http://localhost:15672 (`payments` / `payments`) |
| PostgreSQL | `localhost:5432` (`payments` / `payments`) |

## Локальный запуск без Docker

Нужны Python 3.12, PostgreSQL и RabbitMQ, доступные по адресам из `.env`.

```bash
python -m venv .venv
. .venv/Scripts/activate            # Windows
pip install -e ".[dev]"

alembic upgrade head
python -m uvicorn app.main:app --reload
python -m app.worker.main           # во втором терминале
python scripts/webhook_receiver.py --port 8080   # третий, по желанию
```

## API

### `POST /api/v1/payments`

Заголовки: `X-API-Key` (обязателен), `Idempotency-Key` (8–128 символов, обязателен).

```json
{
  "amount": "1490.50",
  "currency": "RUB",
  "description": "Order #A-10293",
  "metadata": {"order_id": "A-10293"},
  "webhook_url": "http://localhost:8080/hooks/payments"
}
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "2c9f1a4e-6f1c-4f4a-9a2f-0a1b2c3d4e5f",
  "status": "pending",
  "created_at": "2026-01-15T12:00:00Z"
}
```

Заголовки ответа: `Location: /api/v1/payments/<payment_id>` и
`Idempotency-Replayed: true|false` — признак попадания в кэш идемпотентности.

### `GET /api/v1/payments/{payment_id}`

```json
{
  "payment_id": "2c9f1a4e-6f1c-4f4a-9a2f-0a1b2c3d4e5f",
  "amount": "1490.50",
  "currency": "RUB",
  "description": "Order #A-10293",
  "metadata": {"order_id": "A-10293"},
  "webhook_url": "http://localhost:8080/hooks/payments",
  "status": "succeeded",
  "gateway_reference": "ch_9f2c11",
  "failure_reason": null,
  "processing_attempts": 1,
  "created_at": "2026-01-15T12:00:00Z",
  "updated_at": "2026-01-15T12:00:03Z",
  "processed_at": "2026-01-15T12:00:03Z"
}
```

### Ошибки

Единый формат: `{"error": {"code": "...", "message": "...", "details": {...}}}`.

| HTTP | `code` | Когда |
| --- | --- | --- |
| 400 | `missing_idempotency_key` | Нет заголовка `Idempotency-Key` |
| 400 | `invalid_idempotency_key` | Ключ короче 8 или длиннее 128 символов |
| 401 | `invalid_api_key` | Нет или неверный `X-API-Key` |
| 404 | `payment_not_found` | Платёж не найден |
| 409 | `idempotency_key_conflict` | Тот же ключ с другим телом |
| 422 | `validation_error` | Тело не прошло валидацию (`details.errors`) |
| 500 | `internal_error` | Непредвиденная ошибка |

## Идемпотентность

Ключ хранится в `payments.idempotency_key` под уникальным индексом. Рядом лежит
`request_fingerprint` — SHA-256 канонического JSON тела (ключи сортированы, разделители
компактные). Тогда:

- тот же ключ + то же тело → возвращается существующий платёж, новое событие не создаётся;
- тот же ключ + другое тело → `409 idempotency_key_conflict`;
- гонка двух одинаковых запросов разрешается на уровне БД: вторая транзакция падает на
  уникальном индексе, после чего код читает существующую запись и отдаёт её клиенту.

На стороне consumer тот же приём используется при смене статуса: `UPDATE payments SET ...
WHERE id = :id AND status = 'pending'`. Если обновлено `0` строк, платёж уже обработан —
повторная доставка сообщения просто подтверждается.

## RabbitMQ: exchanges, очереди, повторы

| Сущность | Имя | Назначение |
| --- | --- | --- |
| Exchange | `payments` (direct) | Основной обменник |
| Queue | `payments.new` | Рабочая очередь, routing key `payment.created` |
| Exchange | `payments.retry` (direct) | Ретраи |
| Queue | `payments.retry.1` | TTL `PROCESSING_RETRY_BASE_SECONDS`, `x-dead-letter-exchange: payments` |
| Queue | `payments.retry.2` | TTL `×3`, возврат в `payments` |
| Exchange | `payments.dlx` (direct) | Мёртвые сообщения |
| Queue | `payments.dlq` | Всё, что не удалось обработать |

Очереди объявляются один раз при старте (`declare=False` в FastStream + явный
`ensure_topology` через aio-pika), чтобы не зависеть от настроек пассивности. Номер попытки
передаётся заголовком `x-attempt`: retry-очередь `N` публикуется с `x-attempt = N`, TTL
возвращает сообщение в `payments.new`, где воркер подхватывает его без повторного декодирования.

Обработка наткнулась на временную ошибку шлюза (`GatewayUnavailableError`) — сообщение
уходит в retry; терминальный отказ (`PaymentDeclinedError`) — ack без retry; исчерпаны
попытки — публикация в `payments.dlx` с заголовками `x-attempt` и `x-failure-reason`.

## Webhook

Тело (`application/json`):

```json
{
  "event": "payment.succeeded",
  "payment_id": "2c9f1a4e-6f1c-4f4a-9a2f-0a1b2c3d4e5f",
  "status": "succeeded",
  "amount": "1490.50",
  "currency": "RUB",
  "description": "Order #A-10293",
  "metadata": {"order_id": "A-10293"},
  "gateway_reference": "ch_9f2c11",
  "failure_reason": null,
  "created_at": "2026-01-15T12:00:00Z",
  "processed_at": "2026-01-15T12:00:03Z"
}
```

Заголовки: `X-Signature: sha256=<hmac-sha256 тела, WEBHOOK_SIGNING_SECRET>`,
`X-Payment-Event`, `X-Payment-Delivery`.

Проверка на стороне клиента:

```python
import hashlib
import hmac

def verify(raw_body: bytes, signature: str, secret: str) -> bool:
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature.removeprefix("sha256="), expected)
```

Повторяются только сетевые ошибки и коды `408, 425, 429, 500, 502, 503, 504`; любой другой
ответ считается окончательным отказом. Исчерпание попыток не роняет сообщение: результат
уже в БД, а неудача попадает в `webhook_attempts` / `webhook_last_error` и в лог.

## Схема БД

`payments`: `id` (UUID), `idempotency_key` (unique), `request_fingerprint`, `amount`
(NUMERIC(16,2)), `currency`, `description`, `metadata` (JSONB), `webhook_url`, `status`,
`gateway_reference`, `failure_reason`, `processing_attempts`, `webhook_attempts`,
`webhook_last_error`, `created_at`, `updated_at`, `processed_at`.

`outbox_events`: `id`, `aggregate_id`, `event_type`, `routing_key`, `payload` (JSONB),
`status` (`pending` / `published` / `dead`), `attempts`, `available_at`, `published_at`,
`last_error`, `created_at`.

## Конфигурация

Все переменные — в `.env` (шаблон в `.env.example`), читаются через `pydantic-settings`.

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `ENVIRONMENT` | `local` | В `production` потребует явные `API_KEY` и `WEBHOOK_SIGNING_SECRET` |
| `LOG_LEVEL` | `INFO` | Уровень JSON-логов |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | Подключение к БД |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` | `10` / `10` | Пул соединений SQLAlchemy |
| `API_KEY` | `local-dev-api-key` | Значение заголовка `X-API-Key` |
| `WEBHOOK_SIGNING_SECRET` | `local-dev-webhook-secret` | Секрет HMAC для webhook |
| `RABBITMQ_URL` | `amqp://payments:payments@localhost:5672/` | Брокер |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1.0` | Период опроса outbox |
| `OUTBOX_BATCH_SIZE` | `100` | Размер пачки `SKIP LOCKED` |
| `OUTBOX_MAX_ATTEMPTS` | `20` | После скольких неудач событие становится `dead` |
| `OUTBOX_RETRY_BASE_SECONDS` | `5` | База backoff для relay |
| `GATEWAY_MIN_LATENCY_SECONDS` / `GATEWAY_MAX_LATENCY_SECONDS` | `2.0` / `5.0` | Имитация задержки шлюза |
| `GATEWAY_SUCCESS_RATE` | `0.9` | Доля успешных списаний |
| `GATEWAY_UNAVAILABLE_RATE` | `0.0` | Доля временных недоступностей шлюза (для проверки retry) |
| `PROCESSING_MAX_ATTEMPTS` | `3` | Сколько раз пробуем обработать платёж |
| `PROCESSING_RETRY_BASE_SECONDS` | `5` | TTL первой retry-очереди |
| `PROCESSING_RETRY_MULTIPLIER` | `3` | Во столько раз больше TTL следующей очереди |
| `WEBHOOK_TIMEOUT_SECONDS` | `5.0` | Таймаут одного POST |
| `WEBHOOK_MAX_ATTEMPTS` | `3` | Попытки доставки |
| `WEBHOOK_BACKOFF_BASE_SECONDS` | `1.0` | База backoff доставки |

Проверить retry и DLQ целиком:

```bash
GATEWAY_UNAVAILABLE_RATE=1.0 PROCESSING_MAX_ATTEMPTS=3 docker compose up -d --build
```

Один заведомо упавший воркер попытается обработать платёж трижды и отправит сообщение
в `payments.dlq`.

## Тесты

```bash
pip install -e ".[dev]"
pytest                    # только unit
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments_test pytest
```

- `tests/unit` — ретраи, fingerprint и HMAC, topology RabbitMQ, шлюз, webhook-диспетчер,
  контракты схем и чтение заголовка попытки.
- `tests/integration` — HTTP-эндпоинты (включая идемпотентность и коды ошибок), relay outbox
  (успех, отказ брокера, исчерпание попыток), процессор платежей (успех, отказ шлюза,
  повторная доставка, недоступность webhook).

Интеграционные тесты сами накатывают и откатывают миграции на `TEST_DATABASE_URL`.

## Линт и типы

```bash
make lint      # ruff check + mypy app
make format    # ruff format + ruff check --fix
make test
```

CI (`.github/workflows/ci.yml`) гоняет Ruff, `mypy`, миграции и полный `pytest` с
сервисами PostgreSQL и RabbitMQ.

## Структура проекта

```text
app/
  api/          HTTP-слой: схемы, роуты, обработчики ошибок
  core/         Доменные перечисления, константы, исключения, хэширование, retry, время
  db/           Модели, сессия, репозитории
  messaging/    События, topology RabbitMQ, шина публикации
  services/     Платежи, outbox, шлюз, webhook
  worker/       Consumer и обработчик сообщений
alembic/        Миграции
scripts/        Демо-приёмник webhook
tests/          unit + integration
```

## Эксплуатация

- Состояние платежа меняется только `pending → terminal`; повторная доставка события
  безопасна и не списывает деньги дважды.
- Недоставленные события видны в `outbox_events` (`status`, `attempts`, `last_error`) и в
  `payments.dlq`.
- `GATEWAY_SUCCESS_RATE` и `GATEWAY_UNAVAILABLE_RATE` позволяют прогнать сценарии успеха,
  отказа и временной недоступности без внешнего шлюза.
- Перед выкладкой в production задайте `ENVIRONMENT=production`, собственные `API_KEY` и
  `WEBHOOK_SIGNING_SECRET` — иначе приложение не стартует.
