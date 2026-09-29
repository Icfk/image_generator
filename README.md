# Content Generation Service

## Быстрый запуск через Docker

Потребуется установленный и запущенный **Docker Desktop** (или Docker Engine
с Docker Compose).

Откройте терминал в корне проекта — папке с `docker-compose.yml` — и выполните:

```bash
# Создать настройки, не перезаписывая существующий .env
test -f .env || cp .env.example .env
docker compose up -d --build
```

Если `.env` уже существовал, перед запуском проверьте, что в нём установлены
`APP_ENV=development`, `GENERATION_PROVIDER=mock` и
`REQUIRE_WEBHOOK_SIGNATURE=false` для локального бесплатного демо.

Первый запуск скачает образы и установит зависимости, поэтому может занять
несколько минут. PostgreSQL, Redis, API, worker и recovery запускаются
автоматически; миграции БД также применяются автоматически.

- **Фронтенд:** [http://localhost:8000](http://localhost:8000/).
- **Документация API:** [http://localhost:8000/docs](http://localhost:8000/docs).
- **Проверка API:** [http://localhost:8000/health](http://localhost:8000/health).

В интерфейсе нажмите **«Создать пользователя»**, пополните демо-баланс,
указав уникальный ID платежа (например, `demo-payment-001`), затем введите
промпт и нажмите **«Запустить генерацию»**. В mock-режиме показывается локальная
иллюстрация, а не результат нейросети. Ключ Fal.ai и оплата не нужны;
реальные запросы к Fal.ai не выполняются.

Если приложение ещё недоступно, проверьте состояние и логи:

```bash
docker compose ps
docker compose logs --tail=100 api worker recovery
```

После изменения кода повторите `docker compose up -d --build`.
Для остановки выполните `docker compose down`: данные останутся в Docker volumes.
Не добавляйте `-v`, если хотите сохранить пользователей и баланс.

## О проекте и режимах запуска

Тестовое задание: асинхронный FastAPI-сервис и браузерный интерфейс с PostgreSQL,
Redis, балансом пользователей, Fal/mock-провайдерами, фоновыми задачами,
webhook-доставкой, recovery, rate limit и Prometheus-метриками.

`.env.example` — **только локальный demo**: `APP_ENV=development`,
`GENERATION_PROVIDER=mock`, подпись платежа отключена. Compose публикует API
только на `127.0.0.1`. Не выставляйте этот режим в интернет.
При отсутствии `APP_ENV` используется production: запуск отклоняется без
`REQUIRE_WEBHOOK_SIGNATURE=true` и собственного `AUTH_SECRET` длиной минимум
32 символа. Используйте случайный секрет. Эти же проверки действуют в любом
режиме с `GENERATION_PROVIDER=fal`. UI не должен получать секрет платежей.

## Быстрый сценарий

```bash
curl -X POST http://localhost:8000/auth
# взять external_user_id и api_key из ответа
curl -X POST http://localhost:8000/webhooks/top-up -H 'Content-Type: application/json' \
  -d '{"external_user_id":"...","amount":10,"transaction_id":"demo-payment-001"}'
curl -X POST http://localhost:8000/generations/images -H "X-API-Key: ..." \
  -H 'Content-Type: application/json' -d '{"prompt":"a red fox in snow"}'
```

Статусы задачи: `created → queued → processing → completed|failed`. При окончательной
ошибке генерации списанный токен возвращается. Webhook клиента повторяется до 5 раз
с интервалом 15 секунд. `/balance` и `/generations/*` требуют `X-API-Key` и делят
лимит 10 запросов в минуту; превышение блокирует на 60 секунд. Payment webhook
аутентифицируется HMAC-подписью в production, а не пользовательским API-ключом.
Операционные `/health`, `/metrics`, Swagger, `/client-config` (только несекретные
возможности UI), `/auth` и статические файлы открыты. Это явные исключения из
буквального требования «всё кроме /auth»: мониторинг и вход должны работать
до авторизации, платежи поступают от доверенного отправителя. На публичном
развёртывании ограничьте мониторинг сетью/reverse proxy.

## Архитектура и следующие шаги

Структура кода разделена по смысловым слоям:

- `app/domain` — доменные сущности и статусы задач;
- `app/application` — сценарии приложения: пользователи, генерации, провайдер и webhook-доставка;
- `app/infrastructure` — SQLAlchemy-модели, PostgreSQL/Redis и репозитории;
- `app/presentation` — FastAPI-роуты, схемы запросов и зависимости авторизации/rate limit;
- `app/main.py` — только композиция приложения, middleware, lifespan и статика фронтенда.

Composition root `app/infrastructure/container.py` создаёт concrete UoW и сервисы.
Application зависит от `app/domain/ports.py`; repository adapters возвращают
доменные dataclass records, не ORM. Data mapper внутри infrastructure применяет
только изменённые поля перед commit и отбрасывает изменения при rollback.
SQLAlchemy session, запросы, модели, savepoints и commit остаются в infrastructure.
Низкоуровневые repository-тесты могут использовать persistence UoW напрямую;
application/worker/presentation не получают его session.

Очередь задач работает через ARQ/Redis, миграции запускаются командой `alembic upgrade head`, а API и worker запускаются отдельными сервисами Compose. Fal.ai оставлен как явно включаемый режим; mock-режим не требует ключа и не выполняет платных запросов.

Для локальных проверок установите test dependencies:

```bash
pip install -e '.[test]'
pytest -q
black --check app tests alembic
ruff check .
ruff format --check .
```

## Идемпотентное создание generation

Для повторяемого запроса передавайте один и тот же `Idempotency-Key`:

```bash
curl -X POST http://localhost:8000/generations/images \
  -H 'X-API-Key: ...' -H 'Idempotency-Key: order-123-image-1' \
  -H 'Content-Type: application/json' -d '{"prompt":"a red fox in snow"}'
```

Ключ (1–200 символов) уникален в пределах пользователя и общий для image/video
endpoints. Повтор возвращает HTTP 202 и существующие `id`/актуальный `status`,
не списывает токены и не обращается к Redis. Первый запрос с ключом определяет
содержимое задачи; изменение prompt/type при повторе не меняет сохранённую задачу.
Для новой генерации нужен новый ключ. Поле JSON `idempotency_key` тоже поддерживается;
если переданы и поле, и заголовок, их значения должны совпасть, иначе HTTP 400.
Без ключа каждый запрос создаёт отдельную задачу, как и ранее.

Generation, RESERVE ledger, изменение баланса и outbox создаются в одной транзакции.
Уникальная вставка generation внутри savepoint определяет победителя конкурентных
запросов. Проигравший запрос откатывает savepoint и перечитывает существующую задачу.
Только победитель публикует job с ID `generation:<id>`. При timeout enqueue повторная
публикация использует тот же ID; `None` от ARQ означает уже существующую job.
При неопределённом enqueue первый запрос получает 503, outbox остаётся `unknown`,
refund не делается. Повтор с тем же ключом возвращает существующую generation;
восстановлением публикации занимается recovery worker. Это гарантия идемпотентности
создания, а не exactly-once выполнения внешним провайдером.

Перед запуском обновите схему: `alembic upgrade head`.
Миграция `0002_generation_idempotency` добавляет ключ/уникальность базам со старым
вариантом `0001`, сохраняя существующие задачи. Если в базе уже есть дубли ненулевых
ключей, добавление ограничения остановится: данные автоматически не удаляются.

Новые тесты используют реальные SQLAlchemy repositories/UoW и SQLite. Для проверки
конкурентных запросов на PostgreSQL укажите URL **отдельной тестовой БД**:

```bash
TEST_POSTGRES_URL=postgresql+asyncpg://test:test@localhost:5432/test_db \
  .venv/bin/python -m pytest -q
```

Тесты создают и удаляют только свои случайно названные схемы `idempotency_*`;
Redis в API-тестах заменён имитацией с deterministic job IDs и потерей ответа.
Fal.ai не вызывается. Покрыты повтор после commit, параллельные запросы с одним
токеном, сохранение внешней транзакции после IntegrityError, rollback reservation
при сбое outbox и повтор после неопределённого enqueue.

## Конкурентная webhook-доставка

Каждая попытка сначала атомарно захватывает delivery в БД:
`pending → preparing`, `attempts = attempts + 1`. Условный UPDATE проверяет статус,
лимит (не больше 5) и `next_attempt_at`. Только победитель выполняет HTTP;
остальные worker-ы завершаются без отправки. Захват коммитится до HTTP, а запись
результата проверяет номер попытки, чтобы старый worker не перезаписал новую.
Уникальность `generation_id` и savepoint защищают создание самой delivery.
Загрузка payload и проверка DNS происходят в `preparing`; ошибка подготовки
сохраняет pending/failed и не оставляет неопределённый `sending`. Lease подготовки
(`WEBHOOK_PREPARATION_TIMEOUT_SECONDS=30`) восстанавливается recovery после
падения процесса. Условный переход `preparing → sending` проверяет номер попытки
и срок lease, затем коммитится **до HTTP**: опоздавший worker не может отправить
запрос после утраты права. Требуется миграция `0004_webhook_preparation`.

Перед отправкой загружается актуальная generation из БД. Payload сохраняет поля
`id`, `status`, `result`, `error`; устаревший payload из Redis игнорируется.
URL берётся из сохранённой delivery, а не из повторного job. Клиенту передаются
`Idempotency-Key: <generation_id>` и `X-Generation-ID: <generation_id>`.
Получателю следует атомарно сохранять этот ключ вместе с бизнес-операцией и
возвращать 2xx на уже обработанные события.

Разрешены только публичные HTTPS webhook URL на порту 443: без credentials,
локальных/IP special-purpose адресов и фрагментов. Перед каждой попыткой
проверяются **все** ответы DNS. Соединение привязывается к проверенному IP;
TLS SNI/проверка сертификата и Host сохраняют исходное имя. Redirect и прокси
из окружения отключены. Небезопасные старые URL завершаются failed без HTTP.
Сетевой egress firewall рекомендуется как дополнительный уровень защиты.

2xx завершает доставку как `delivered`. Ошибка сохраняет `last_error` и переводит
delivery в `pending` с `next_attempt_at` через 15 секунд (по умолчанию), а пятая
ошибка — в `failed`. Terminal-состояния не отправляются повторно. Первичная
доставка и retry используют единый ID job `delivery:<generation_id>:<attempts>`;
даже дубли Redis jobs не обходят атомарный захват в БД.

Ограничение: при падении процесса/БД после перехода в `sending`, до сохранения результата,
запись останется `sending` с неизвестным исходом. Автоматического истечения lock
нет: это могло бы разрешить отправку одновременно с ещё работающим запросом.
Такие записи требуют сверки с получателем и ручного восстановления **после
подтверждения остановки прежнего worker-а**. Exactly-once HTTP без дедупликации
на стороне клиента не гарантируется. Потерянные retry jobs для `pending` теперь
восстанавливает отдельный recovery worker (описан ниже); `sending` он не трогает.

Тесты `tests/test_delivery_concurrency.py` используют настоящие repositories/UoW,
HTTP MockTransport и управляемые часы: ожиданий по 15 секунд и вызовов Fal.ai нет.
`TEST_POSTGRES_URL` включает те же concurrency-тесты на PostgreSQL.

## Recovery после падения worker/Redis

В Compose добавлен сервис `recovery` (`python -m app.recovery_worker`). Он делает
ограниченный проход каждые 15 секунд. Он не вызывает Fal.ai и не отправляет HTTP
webhook самостоятельно — только восстанавливает состояние и публикации ARQ.

| Состояние в БД | Действие recovery |
| --- | --- |
| `queued`, publication отсутствует | Создать одну publication и опубликовать job |
| Publication `pending` / `unknown` для queued | Сверить Redis; опубликовать отсутствующую job, не refund |
| Publication `published`, но Redis потерял job | Восстановить ту же job с тем же ID |
| `processing` старше timeout, есть `provider_job_id` | Сбросить право старого worker-а, поставить polling в очередь без submit |
| `processing` старше timeout, job ID отсутствует | `failed` + один идемпотентный refund; **не повторять submit** |
| `completed` / `failed` | Не запускать generation; восстановить отсутствующую delivery, если известен webhook URL |
| Delivery `pending`, срок наступил | Восстановить retry job, сохранив attempts и актуальный payload-loader |
| Delivery `preparing`, lease истёк | Вернуть в pending или failed после 5 попыток; старый владелец не получает право HTTP |
| Delivery `sending` / `delivered` / `failed` | Не переотправлять автоматически |

Recovery использует PostgreSQL `SELECT FOR UPDATE SKIP LOCKED`. Два recovery
worker-а не публикуют одну запись одновременно. Блокировка удерживается только
на время короткой транзакции и ограниченных по времени Redis-операций, не на время
provider call. Job IDs детерминированы, включая successor ID для случая, когда
ARQ уже хранит завершённый результат старой job. Это позволяет безопасно повторить
публикацию при потере Redis ACK или неизвестном результате DB commit. Обход
пакетный; порядок последней проверки предотвращает голодание более новых записей.

Generation теперь сохраняет `processing` до provider call и `provider_job_id` /
`provider_context` сразу после submit, **до polling**. Отдельный `run_token`
защищает checkpoint/result от записи устаревшим worker-ом. Результат и создание
delivery сохраняются вместе. `webhook_url` сохраняется при создании generation,
поэтому потеря Redis args не теряет адрес получателя. Реальные Fal submit/poll
не выполняются в тестах; используется HTTP MockTransport. Polling получает
результат отдельным GET согласно [Fal queue API](https://fal.ai/docs/documentation/model-apis/inference/queue).

Явная безопасная policy для неопределённого submit: если provider мог принять
запрос, но job ID не попал в БД, повторный submit запрещён. По истечении timeout
задача становится failed и клиенту возвращается резерв. Возможно, провайдер уже
выполнил/оплатил работу — сверка такого случая остаётся операционной задачей.
Это выбор в пользу отсутствия второй платной генерации, не гарантия exactly-once
у внешнего провайдера. Смена провайдера для уже сохранённого checkpoint запрещена.

### Запуск и наблюдаемость

Перед обновлением остановите старые worker-ы: они ещё не соблюдают `run_token`.
Примените `alembic upgrade head` (включая `0004_webhook_preparation`),
затем запустите новую версию API, worker и recovery. В Compose API запускает
миграцию сам, worker/recovery ждут его healthcheck. Dockerfile теперь включает
Alembic-конфигурацию и миграции.

Настройки в `.env.example`:

- `PROCESSING_TIMEOUT_SECONDS=600` — срок processing; должен превышать timeout
  provider operation и ARQ job (по умолчанию 300 и 330 секунд).
- `RECOVERY_INTERVAL_SECONDS=15`, `RECOVERY_BATCH_SIZE=100`.
- `RECOVERY_HEALTH_MAX_AGE_SECONDS=90`, `RECOVERY_METRICS_PORT=8001`.

Heartbeat записывается в Redis по имени контейнера **только после успешного
прохода БД + очереди** и имеет TTL. `python -m app.recovery_worker --healthcheck`
возвращает ненулевой код при отсутствии/просрочке heartbeat или недоступности
Redis. Compose выполняет этот healthcheck автоматически. Docker сам по себе
не перезапускает живой контейнер только из-за unhealthy: настройте алертинг.

Метрики recovery доступны отдельно на `http://recovery:8001/metrics` внутри
Compose-сети: `recovery_actions_total{action,outcome}`, `recovery_cycles_total`
и `recovery_last_success_timestamp_seconds`. API `/metrics` не агрегирует другой
процесс: Prometheus должен опрашивать recovery отдельно.

Worker также имеет отдельный exporter: `http://worker:8002/metrics`
(`WORKER_METRICS_PORT`). Он запускается/закрывается ARQ startup/shutdown hooks.
На нём доступны generation/provider/delivery counters этого процесса. Для всех
метрик опрашивайте API `api:8000`, worker `worker:8002` и recovery `recovery:8001`
как отдельные scrape targets. При масштабировании опрашивайте каждый экземпляр;
один ARQ-процесс на контейнер. Порты exporters не публикуются на хост.
`generation_cost_total` считает списанную стоимость только успешных генераций
в условных токенах (сейчас 1 за задачу), а не фактический долларовый счёт Fal.
API labels используют шаблон маршрута, а неизвестные пути/методы — постоянные
значения, не UUID и пользовательские строки.

Rate limit выполняется одним атомарным Redis Lua script, включая TTL и блокировку.
Старые счётчики/блокировки без TTL восстанавливаются при следующем обращении.
Конфигурация рассчитана на standalone Redis, как в Compose, не Redis Cluster.

Fal-режим не переключается на MockProvider. Вместо этого подключён реальный
Fal fallback: по умолчанию тот же поставщик/модели, что разрешено заданием.
`FAL_FALLBACK_API_KEY` позволяет выбрать другой аккаунт, `FAL_FALLBACK_MODELS`
задаёт JSON mapping режимов (`text-to-image`, `image-to-image`, `text-to-video`,
`image-to-video`) к совместимым моделям. Это не независимый от Fal outage резерв:
для него нужен отдельный provider adapter. Временная ошибка до подтверждённого
submit или открытый circuit переключают запрос на fallback. Permanent error
сразу завершает задачу с refund. После submit переключение запрещено: checkpoint
хранит имя provider, job ID и URLs; retry продолжает polling того же provider,
включая fallback. Неопределённый submit не повторяется, даже ради failover.
Mock доступен только как явно выбранный провайдер. Отмена и утрата claim освобождают
half-open probe; fallback errors учитываются метриками и логируются без текста
ответа/секретов. API, worker и recovery используют `LOG_DIR` и ротацию 7 дней.

Для интеграционных тестов задайте `TEST_POSTGRES_URL` и `TEST_REDIS_URL`, указывающие
на **изолированные тестовые** БД/Redis, и выполните `.venv/bin/pytest -q`.
Без PostgreSQL два теста row locks пропускаются: SQLite не поддерживает FOR UPDATE.
Redis-тест удаляет только собственную job для моделирования потери volatile state.

Ограничения старых данных: миграция не может восстановить webhook URL и job ID,
которые прежняя версия никогда не записывала. Для них применяются правила выше;
неизвестные URL автоматически не угадываются. `sending` delivery по-прежнему
требует ручной сверки перед возможным повтором.

## Идемпотентное пополнение баланса

`POST /webhooks/top-up` требует `transaction_id` или `idempotency_key` от отправителя.
Сервис не генерирует ключ платежа. Если переданы оба поля, сохранён прежний приоритет:
используется `idempotency_key`. Для одной операции отправитель обязан сохранять
одинаковый выбранный ключ при каждом повторе, в том числе после timeout/HTTP 500.

Ключ глобально уникален (не только в пределах пользователя), не пустой и не состоит
из пробелов. Длина — максимум 192 символа: вместе с префиксом ledger `deposit:`
значение должно помещаться в существующий `VARCHAR(200)`. Отсутствующий/некорректный
ключ возвращает 422, не меняя БД. В тестовом UI ID вводится явно и не сбрасывается
после запроса; для следующего реального пополнения нужен новый ID.

Первая обработка возвращает 200 с `duplicate: false`; повтор с тем же пользователем
и суммой — 200 с `duplicate: true` и текущим балансом **этого** пользователя.
Другая сумма или пользователь с уже занятым ключом дают 409 с общим сообщением
`Idempotency key conflict`, без баланса владельца ключа.

Уникальная вставка payment event выполняется первой внутри savepoint. Победитель
берёт `SELECT FOR UPDATE` на пользователя и сохраняет увеличение balance и DEPOSIT
ledger в одной транзакции с event. Проигравший откатывает только savepoint,
перечитывает событие и сверяет пользователя/сумму. Другие IntegrityError не
маскируются под duplicate. При неизвестном результате COMMIT следующий запрос с
тем же ключом либо видит уже завершённый платёж, либо выполняет его один раз.

`REQUIRE_WEBHOOK_SIGNATURE` сохранён. При `true` нужен `X-Webhook-Signature`:
HMAC-SHA256 с `AUTH_SECRET` от UTF-8 `json.dumps(TopUpRequest(...).model_dump(),
sort_keys=True)` — прежний формат, включая отсутствующее альтернативное поле
ключа как `null`. Неверная/отсутствующая подпись даёт 401 для валидного payload.
При `false` подпись не проверяется; это только локальный тестовый режим, не
безопасная настройка публичного payment endpoint. UI не получает секрет подписи.

Payment-доработка использует существующие unique constraints и не требует новой
миграции. Для текущей версии целиком выполните `alembic upgrade head`.

## Четыре режима генерации и интерфейс

Публичные endpoints не изменены. Наличие `image_url` выбирает входной режим:

| Endpoint | image_url | Модель по умолчанию |
| --- | --- | --- |
| `/generations/images` | нет | `fal-ai/flux/dev` |
| `/generations/images` | есть | `fal-ai/flux/dev/image-to-image` |
| `/generations/videos` | нет | `fal-ai/wan-25-preview/text-to-video` |
| `/generations/videos` | есть | `fal-ai/wan-25-preview/image-to-video` |

Настройки: `FAL_MODEL_IMAGE`, `FAL_MODEL_IMAGE_TO_IMAGE`, `FAL_MODEL_TEXT_TO_VIDEO`,
`FAL_MODEL_VIDEO` соответственно. Для видео максимум 1500 символов промпта.
Маршрутизация сверена со схемами
[image-to-image](https://fal.ai/models/fal-ai/flux/dev/image-to-image/api) и
[text-to-video](https://fal.ai/models/fal-ai/wan-25-preview/text-to-video/api).
Тесты используют только HTTP MockTransport, без платных вызовов.

UI содержит явный выбор всех четырёх режимов и проверяет обязательное изображение.
Ключ generation и исходный payload записываются в localStorage **до HTTP**.
Двойное нажатие блокируется. При 503/504, timeout или потере ответа кнопка
«Повторить тот же запрос» сохраняет ключ и параметры, в том числе после reload.
После подтверждения UI хранит task ID и только читает статус; новая генерация
требует явного действия «Новая задача». Для разных пользователей запросы раздельны.
Очистка browser storage теряет ключи — не очищайте его до разрешения неопределённого
исхода. API-ключ тоже хранится локально: используйте только доверенный браузер.

Опрос выполняется раз в 12 секунд, при 429 учитывается `Retry-After`.
Старые результаты и ошибки очищаются перед новой задачей, URL результата
проверяется перед созданием ссылки. Mock возвращает локальную SVG-иллюстрацию;
это демонстрация workflow, **не настоящее изображение/видео от нейросети**.
В Fal-режиме UI предупреждает о платной генерации; demo top-up скрыт при
обязательной подписи, секрет HMAC никогда не передаётся браузеру.

## Таймауты и финансовая атомарность

API имеет общий deadline `API_TIMEOUT_SECONDS=4.5` (настройка не может превышать
4.5 секунды); публикация Redis ограничена `ENQUEUE_TIMEOUT_SECONDS=1`.
PostgreSQL pool/connect/command и Redis socket operations также имеют timeout.
При зависании обработчик отменяется и отдаёт 504; при неопределённом commit нужно
повторять **тот же idempotency key**. Если enqueue не подтверждён, БД сохраняет
pending/unknown publication, резерв не возвращается, восстановление делает recovery.
Это ограничение серверной работы, не гарантия сетевой задержки клиента под любой
нагрузкой; отдельный нагрузочный тест не проводился.

Refund подавляет только конфликт уникального `external_key` с подтверждённой
существующей записью. Любой другой IntegrityError пробрасывается и откатывает
вместе failed-статус и баланс. Покрыты реальная ошибка ограничения БД, конкуренция
PostgreSQL, duplicate и потеря подтверждения commit до/после записи.

## Проверки перед отправкой

Backend и миграции:

```bash
python -m venv .venv
.venv/bin/pip install -e '.[test]'
APP_ENV=test .venv/bin/python -m compileall app tests alembic
.venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/black --check app tests alembic
```

Для полного прогона добавьте `TEST_POSTGRES_URL` и `TEST_REDIS_URL` от отдельных
тестовых сервисов. Без них integration tests явно skipped.

Браузерные тесты используют mock API, а статические файлы загружают из настоящего
приложения; ни один запрос Fal.ai не разрешён. Node нужен **только для тестов и
форматирования**, production UI не требует сборки:

```bash
npm ci
npm test
npx playwright install chromium
npm run test:e2e
npm run format:check
```

Playwright запускает отдельный localhost API на 8871, с `APP_ENV=test` и mock.
Для упаковки не включайте `.env`, `.venv`, `node_modules`, caches, logs,
test-results и реальные секреты. Сдача — ссылка на GitHub с README, Dockerfile,
Compose, миграциями, тестами, `package-lock.json` и `.env.example`.
Публикация в GitHub не выполняется автоматически.
