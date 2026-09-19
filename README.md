# Лабораторная 1 по BDA — Airflow + PostgreSQL

Окружение под задание из `Task1_BDA.docx`: **Задание 1** (витрина по заказчикам) и
**Задание 2** (схема `external_source` и загрузка трёх CSV). Задание 3 (logs + MongoDB) —
позже, когда появится остаток текста: в docx он обрывается на коллекции `Clients`.

Сами задания пишутся в `dags/` и `sql/`; в `reference/` лежит рабочий вариант решения,
отключённый от Airflow, — для сверки, если где-то застрял.

---

## 1. Как читать сам docx

Документ сбивает с толку, потому что в нём **два варианта одного и того же задания подряд**:

| Блок | Задание 1 — таблицы | Задание 2 — таблицы |
|------|--------------------|---------------------|
| Первый (эталон-пример из курса) | `craft_market_masters_products`, `craft_market_orders_customers`, `craft_market_craftsmans`, `craft_market_customers`, `craft_market_orders` | `user_order_log`, `user_activity_log`, `customer_research` |
| Второй (**твой вариант**, ER-диаграмма на второй картинке) | `master`, `product`, `master_product`, `order`, `client` | `order_statuses`, `user_behavior`, `marketing_research` |
| Задание 3 | таблица `logs` + MongoDB (`clients`, `orders`, `restaurant`) | — |

Первый блок — это пример из курса Яндекс.Практикума «магазин изделий ручной работы»,
второй — то же самое, переименованное под твой вариант. **Делаем по второму блоку**,
первый используем как подсказку там, где второй недоговаривает.

## 2. Что вообще требуется

Задание 1 — классическая задача «OLTP → витрина»:
есть нормализованная база магазина хендмейда (мастера, товары, заказчики, заказы),
надо **одним SQL-запросом** посчитать витрину: одна строка = один заказчик за один отчётный месяц.
Airflow нужен, чтобы этот пересчёт запускался по расписанию и его можно было перезапускать.

Задание 2 — «подключить внешний источник»: три CSV-файла нужно положить в отдельную схему
`external_source` внутри того же хранилища, причём **в целевые таблицы берутся не все колонки файла,
а только перечисленные в задании**. Это и есть ETL: extract (файл) → staging (как есть) →
transform (выбор нужных колонок) → load.

## 3. Модель данных (Задание 1)

Из ER-диаграммы в docx, DDL — `reference/sql/task1/01_ddl_source.sql`:

```
master (master_id, name, phone, address, birthday)
product (product_id, name, description, type)        -- type = категория товара
master_product (id, master_id, product_id, price)    -- цена живёт ЗДЕСЬ, не в заказе
client (client_id, name, phone, address, birthday, email)
orders (order_id, master_id, product_id, client_id, order_date, delivery_date, status)
```

Три вещи, которые в документе не сказаны, и я их решил сам (на защите это спросят):

1. **`order` — зарезервированное слово в PostgreSQL.** Таблица названа `orders`,
   но есть вьюха `source."order"` — если препод потребует именно такое имя, запрос по нему отработает.
2. **`client.email` в диаграмме нет**, а в списке колонок витрины «электронная почта заказчика» есть.
   Колонку добавил.
3. **Домен статусов не задан.** Взял `created` / `in_progress` / `delivery` / `done`.
   «Незавершённый заказ» — это не статус, а всё, что не `done` (вычисляется).

## 4. Колонки витрины и откуда что берётся

Таблица — `mart.customer_report` (`reference/sql/task1/02_ddl_mart.sql`). Разбор по пунктам задания:

| Пункт задания | Колонка | Как считается |
|---|---|---|
| идентификатор записи | `id` | суррогатный `bigserial` |
| идентификатор заказчика | `client_id` | `client.client_id` |
| Ф. И. О. / адрес / дата рождения / почта | `client_name`, `client_address`, `client_birthday`, `client_email` | из `client` |
| сумма всех оплаченных заказов клиента | `total_paid_amount` | `sum(price)` по заказам со статусом `done`, **накопительно** (см. ниже) |
| количество заказов за месяц | `orders_count` | `count(*)` по заказам месяца |
| средняя стоимость заказа за месяц | `avg_order_value` | `avg(price)` |
| медианное время выполнения, дней | `median_completion_days` | `percentile_cont(0.5) WITHIN GROUP (ORDER BY (delivery_date - order_date))` по завершённым, результат `::numeric` (см. ниже) |
| самая популярная категория | `top_product_type` | `mode() WITHIN GROUP (ORDER BY product.type)` |
| самый популярный мастер | `top_master_id` | `mode() WITHIN GROUP (ORDER BY master_id)` |
| создано / в изготовлении / в доставке / завершено | `orders_created_count`, `orders_in_progress_count`, `orders_delivery_count`, `orders_done_count` | `count(*) FILTER (WHERE status = ...)` |
| незавершённых | `orders_not_done_count` | `count(*) FILTER (WHERE status <> 'done')` |
| отчётный период | `report_year`, `report_month` | из `order_date` |

Тонкости, за которые цепляются на защите:

* **«сумма всех оплаченных заказов» — единственная метрика без слов «за месяц»**, поэтому она
  накопительная, а не месячная (в первом варианте задания было «сумма, которую потратил заказчик …
  за месяц» — формулировки разные). Если препод скажет «за месяц» — правка в одну строку.
* **Цена заказа берётся джойном `orders → master_product` по паре (master_id, product_id)**,
  потому что в `order` цены нет. Это неочевидное место модели.
* **`percentile_cont` возвращает `double precision`, а `round(x, 2)` есть только для `numeric`** —
  без каста `round((percentile_cont(...) FILTER (...))::numeric, 2)` запрос падает с
  `ERROR: function round(double precision, integer) does not exist`. Я на этом споткнулся при проверке.
* `mode()` при равенстве количеств выбирает произвольное значение — ровно то, что разрешает
  задание («возьмите любого» мастера).
* **Импорт оператора**: в Airflow 2.10.5 `airflow.providers.postgres.operators.postgres` уже нет,
  `SQLExecuteQueryOperator` берётся из `airflow.providers.common.sql.operators.sql`.
* **Идемпотентность**: загрузка сначала удаляет строки отчётного периода, потом вставляет заново.
  Без этого повторный запуск DAG'а в Airflow продублирует витрину — это первое, что ломается.
* В первом варианте задания была «сумма, которую заработала платформа (10%)» — **в твоём варианте
  этого пункта нет**, я его не добавлял.

## 5. Задание 2 — маппинг файлов в таблицы

| Файл (все колонки) | Целевая таблица `external_source` (только эти колонки) |
|---|---|
| `order_statuses.csv`: id, date_time, city_id, city_name, client_id, first_name, last_name, item_id, item_name, quantity, payment_amount | `order_statuses`: date_time, client_id, quantity, payment_amount |
| `user_behavior.csv`: id, date_time, behavior_id, **user_id**, quantity | `user_behavior`: date_time, **client_id** |
| `marketing_research.csv`: date_id, category_id, sales_qty, sales_amt | `marketing_research`: date_id, sales_qty, sales_amt |

Обрати внимание на второй строке: в файле колонка называется `user_id`, а в целевой таблице
задание требует `client_id` — переименование делается на шаге transform
(`reference/sql/task2/02_staging_to_external_source.sql`).

Файл целиком грузится в схему `staging` (таблицы `*_raw`), оттуда нужные колонки переливаются
в `external_source`. Так видно и «сырые» данные, и результат — это то, что от ETL и ждут.

## 6. Что в проекте

Проект — это **окружение**. Задания пишешь сам: свои DAG-и в `dags/`, свой SQL в `sql/`.

```
docker-compose.yml     Airflow 2.10.5 (LocalExecutor) + 2 PostgreSQL:
                         postgres-airflow — метаданные Airflow (лабу туда НЕ кладём)
                         postgres-dwh     — хранилище: схемы source / staging / external_source / mart
Makefile               make up / gen / ref-ddl / ref-seed / psql — всё управление отсюда
.env                   порты, логины, версия образа
dags/
  smoke_test_dwh.py    проверка окружения: коннект к хранилищу + примонтированные папки
  <твои DAG-и сюда>
sql/                   <твой SQL сюда> (примонтирован в контейнеры как /sql и /opt/airflow/sql)
data/generate_data.py  генератор тестовых данных (только stdlib, без pip)
data/source/*.csv      данные для Задания 1
data/external_source/*.csv  три файла Задания 2 — с колонками ровно как в docx
reference/             мой вариант решения — НЕ подключён к Airflow, лежит как подсказка
  dags/*.py            два DAG-а (витрина и загрузка CSV)
  sql/task1/*.sql      DDL источника, DDL витрины, загрузка данных, запрос витрины
  sql/task2/*.sql      DDL external_source + transform
```

`reference/` Airflow не сканирует — в UI эти DAG-и не появятся, пока сам не скопируешь их в `dags/`
(папка примонтирована как `/opt/airflow/reference`, поэтому пути к SQL внутри них рабочие).

## 7. Как запустить

```bash
make up        # Airflow UI: http://localhost:8080, логин airflow / пароль airflow
make gen       # сгенерировать CSV (уже сгенерированы, можно пропустить)
make ref-ddl   # создать схемы и таблицы (DDL из reference/, чтобы было на чём тренироваться)
make ref-seed  # залить данные магазина в схему source
make drop      # снести все схемы лабы, если хочешь начать с чистого листа
```

Подключение к хранилищу `dwh_postgres` уже прописано в Airflow через переменную окружения —
руками в UI ничего создавать не надо, в своём DAG-е просто бери
`PostgresHook(postgres_conn_id="dwh_postgres")` или `SQLExecuteQueryOperator(conn_id="dwh_postgres")`.

Хранилище снаружи: `localhost:5433`, БД `dwh`, пользователь `dwh`, пароль `dwh`
(DBeaver/DataGrip), либо `make psql`.

## 8. Статус окружения (проверено запуском)

* контейнеры подняты, Airflow UI отвечает, `smoke_test_dwh` отработал зелёным:
  коннект к PostgreSQL 16.15 есть, папки `sql` и `data` видны изнутри Airflow;
* `make ref-ddl` + `make ref-seed` отработали: в `source` лежат 30 мастеров, 120 товаров,
  223 связки мастер-товар, 200 клиентов, 4000 заказов за январь–июнь 2024;
* схемы `source`, `staging`, `external_source`, `mart` созданы;
* оба DAG-а из `reference/` прогнаны через `airflow dags test` целиком и отработали зелёными:
  Задание 2 — 3000 / 5000 / 1820 строк доехали до `external_source`,
  Задание 1 — витрина за март/апрель/май 2024 (196 / 195 / 188 строк), проверки прошли;
* после проверки целевые таблицы (`mart`, `staging`, `external_source`) очищены, а `source`
  оставлен заполненным — начинаешь с чистого листа, но данные для запросов уже есть.

## 9. Что глянуть по теме

* `percentile_cont(... ) WITHIN GROUP (ORDER BY ...)` и `mode() WITHIN GROUP` —
  ordered-set aggregate functions в PostgreSQL, ими медиана и «самая популярная категория»
  считаются в одну строку.
* `count(*) FILTER (WHERE ...)` — считать разные статусы одним проходом вместо пяти подзапросов.
* В Airflow: `PostgresHook.copy_expert` (быстрая загрузка CSV через `COPY`),
  `SQLExecuteQueryOperator`, `template_searchpath`, `catchup`/`logical_date` — то, чем
  витрина пересчитывается за конкретный месяц.
* Идемпотентность загрузки (delete-by-period + insert) — главный вопрос «почему у вас
  повторный запуск не ломает витрину».
