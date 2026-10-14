# Payments Service

Асинхронный сервис процессинга платежей: FastAPI + SQLAlchemy 2.0 (async) + PostgreSQL + RabbitMQ (FastStream) + Alembic.

Сервис принимает платёж, сохраняет его вместе с событием в outbox, relay публикует событие в RabbitMQ,
consumer эмулирует платёжный шлюз, сохраняет результат и отправляет webhook. Ошибки ретраятся с
экспоненциальной задержкой, после 3 неудачных попыток сообщение уходит в DLQ.

## Запуск

```bash
cp .env.example .env
# заполнить пароли и ключи, например так:
for k in POSTGRES_PASSWORD RABBITMQ_PASSWORD API_KEY WEBHOOK_SECRET; do
  sed -i.bak "s/^$k=$/$k=$(openssl rand -hex 32)/" .env
done && rm .env.bak

docker compose up --build -d
docker compose logs -f consumer outbox-relay
```

API: `http://127.0.0.1:8000`. Миграции применяет one-shot сервис `migrate`, остальные сервисы ждут его завершения.
Swagger (`/docs`) по умолчанию выключен, включается через `DOCS_ENABLED=true`.

## Примеры

```bash
export API_KEY=$(grep ^API_KEY= .env | cut -d= -f2)
```

Создание платежа (для проверки webhook удобно взять URL с https://webhook.site):

```bash
curl -i -X POST http://127.0.0.1:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://webhook.site/<your-uuid>"
      }'
```

```http
HTTP/1.1 202 Accepted

{"payment_id": "3f6c...", "status": "pending", "created_at": "2026-10-14T10:00:00.000000Z"}
```

Повтор с тем же `Idempotency-Key` и тем же телом вернёт тот же платёж (новый не создаётся).
Тот же ключ с другим телом — `409 Conflict`.

Получение платежа:

```bash
curl -s http://127.0.0.1:8000/api/v1/payments/<payment_id> -H "X-API-Key: $API_KEY"
```

```json
{
  "payment_id": "3f6c...",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "https://webhook.site/<your-uuid>",
  "created_at": "2026-10-14T10:00:00.000000Z",
  "processed_at": "2026-10-14T10:00:03.512000Z",
  "webhook_delivered_at": "2026-10-14T10:00:03.700000Z"
}
```

Коды ответов: `401` — нет/неверный `X-API-Key`, `404` — платёж не найден, `409` — конфликт idempotency key,
`422` — ошибка валидации (в т.ч. отсутствует `Idempotency-Key`).

Состояние очередей:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

## Архитектура

```
client ──POST──> api ──(1 транзакция)──> payments + outbox
                                              │
                         outbox-relay ────────┘ SELECT ... FOR UPDATE SKIP LOCKED
                              │ publish (publisher confirms) → published_at = now()
                              v
           exchange "payments" ──payments.new──> queue payments.new ──> consumer
                                                                           │ 1. lock payment, если pending:
                                                                           │    эмуляция 2–5 c, 90% succeeded / 10% failed
                                                                           │ 2. POST webhook
                                                                           │ 3. webhook_delivered_at = now()
                              ошибка, попытки есть ┌───────────────────────┤
                                                   v                       │ ошибка после 3-й попытки
    payments.retry ──> payments.new.retry.1 (TTL 2s) ─┐                    v
                   └─> payments.new.retry.2 (TTL 4s) ─┴─(DLX)─> payments   payments.dlx ──> payments.new.dlq
```

Сервисы в `docker-compose.yml`: `postgres`, `rabbitmq`, `migrate`, `api`, `outbox-relay`, `consumer`.

### Outbox
Платёж и событие `payment.created` пишутся в одной транзакции — событие не потеряется и не появится без платежа.
Relay выбирает неопубликованные строки с `FOR UPDATE SKIP LOCKED` (можно запускать несколько реплик),
публикует с publisher confirms и только после подтверждения брокера проставляет `published_at`.
Доставка at-least-once: при падении между confirm и commit событие уйдёт повторно — consumer к этому устойчив.

### Идемпотентность
- **API**: уникальный индекс на `idempotency_key` + SHA-256 отпечаток тела запроса. Гонку параллельных запросов
  с одним ключом разрешает уникальный индекс: проигравший получает `IntegrityError` и возвращает уже созданный платёж.
- **Consumer**: строка платежа блокируется `SELECT ... FOR UPDATE`, списание выполняется только из статуса `pending`.
  Повторная доставка или ретрай не списывают второй раз, а только досылают webhook, если он ещё не доставлен
  (`webhook_delivered_at`).
- **Webhook**: доставка at-least-once, получатель дедуплицирует по `X-Webhook-Id` (= `payment_id`).

### Retry и DLQ
Consumer работает с ручным ack. При ошибке (webhook не 2xx/таймаут, недоступна БД и т.п.) сообщение
перепубликуется в delay-очередь `payments.new.retry.N` с заголовком `x-attempt`; по истечении TTL RabbitMQ
через DLX возвращает его в `payments.new`. Задержки: 2 c, 4 c (`RETRY_BASE_DELAY_MS * 2^(N-1)`).
После 3-й неудачной попытки сообщение публикуется в `payments.new.dlq` с заголовком `x-last-error`.
Невалидные сообщения отклоняются (`reject`) и попадают в ту же DLQ через `x-dead-letter-exchange` основной очереди.
Ретраи через брокер, а не `sleep` в consumer — медленный webhook не блокирует обработку других сообщений.

> Параметры `x-message-ttl` фиксируются при объявлении очереди. Если меняете `MAX_ATTEMPTS`/`RETRY_BASE_DELAY_MS`
> на работающем стенде — удалите очереди `payments.new.retry.*` (или `docker compose down -v`).

### Webhook
`POST` на `webhook_url` с JSON платежа и заголовками:

| Заголовок | Значение |
|---|---|
| `X-Webhook-Id` | `payment_id` |
| `X-Webhook-Timestamp` | unix time |
| `X-Webhook-Signature` | `sha256=` + HMAC-SHA256(`WEBHOOK_SECRET`, `"{timestamp}.{body}"`) |

Проверка на стороне получателя:

```python
expected = hmac.new(secret, f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
assert hmac.compare_digest(f"sha256={expected}", signature)
```

## Безопасность

- **Сеть.** Postgres и RabbitMQ не публикуют порты и живут в `internal`-сети без выхода наружу.
  Наружу смотрит только API, и только на `127.0.0.1`. Выход в интернет есть только у consumer (для webhook).
- **SSRF.** `webhook_url` — только `http`/`https`, без credentials; запрещены приватные/loopback/link-local/
  зарезервированные IP и внутренние имена (`postgres`, `rabbitmq`, `*.local`, ...). При отправке consumer
  дополнительно проверяет каждый IP, в который резолвится хост, прямо в резолвере HTTP-клиента
  (защита от DNS rebinding), не следует редиректам и игнорирует proxy из окружения.
  Для локальной отладки ограничение снимается `WEBHOOK_ALLOW_PRIVATE_NETWORKS=true`.
- **Аутентификация.** `X-API-Key` на всех эндпоинтах, сравнение за константное время, ключ не короче 32 символов.
- **Секреты** только из `.env` (в `.gitignore`); compose не стартует с пустыми значениями. Каждый сервис получает
  только нужные ему переменные.
- **Контейнеры** приложения: non-root пользователь, read-only FS, `cap_drop: ALL`, `no-new-privileges`.

## Структура

```
app/
  main.py            FastAPI-приложение
  api/               роуты и зависимости (auth, сессия БД)
  payments.py        создание платежа: idempotency + outbox
  models.py          SQLAlchemy-модели payments, outbox
  schemas.py         Pydantic-схемы API, события и webhook
  topology.py        exchanges/queues/DLX RabbitMQ
  outbox_relay.py    relay outbox → RabbitMQ
  consumer/          consumer: обработка, webhook, retry/DLQ
  url_safety.py      валидация webhook URL (SSRF)
migrations/          Alembic
```
