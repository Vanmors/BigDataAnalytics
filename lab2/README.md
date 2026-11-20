# Lab 2: Data Vault on Vertica

Условие: [задание_2.md](задание_2.md), схема модели: [img_1.png](img_1.png).

Стенд: Airflow 2.10.5 (LocalExecutor), PostgreSQL для метаданных Airflow и
одноузловая Vertica 25.4 Community Edition.

```sh
cp .env.example .env   # once
make up                # first start creates the Vertica database, about 30 s
make vsql              # SQL console of the warehouse
make down              # stop, data is kept
make reset             # stop and delete all data
```

| Что                           | Где                                                  |
|-------------------------------|------------------------------------------------------|
| Airflow                       | http://127.0.0.1:8081, `airflow` / `airflow`         |
| Vertica                       | `127.0.0.1:5434`, база `lab2`, `dbadmin` / `vertica` |
| Подключение Airflow к Vertica | `vertica_dwh`                                        |

## Vertica

Используется официальный образ `opentext/vertica-k8s:25.4.0-0-multiarch`: он работает
на arm64 и amd64 без эмуляции. 25.4 - последняя версия с бесплатной лицензией
Community Edition (до 1 ТБ, до 3 узлов), начиная с 26.1 она не принимается.

Образ рассчитан на Kubernetes-оператор, поэтому [vertica/entrypoint.sh](vertica/entrypoint.sh)
сам выполняет его работу: генерирует TLS-сертификаты, запускает node management agent
и через `vcluster` создаёт базу при первом старте или запускает существующую.
При остановке контейнера база останавливается штатно. Каталог, данные и сертификаты
лежат в томе `vertica-data`.

Vertica хранит IP узла в каталоге, поэтому у контейнера фиксированный адрес
`172.28.0.10` в сети стенда.

## DAG и витрины

Два DAG, оба пишут в Vertica через подключение `vertica_dwh`.

| DAG            | Расписание                           | Что делает                                                               |
|----------------|--------------------------------------|--------------------------------------------------------------------------|
| `sn_dwh_load`  | `0 3 * * *` (раз в сутки, 03:00 UTC) | CSV соцсети → `stg` → Data Vault (`dds`) → аналитические витрины (`cdm`) |
| `sn_sys_marts` | `*/30 * * * *` (каждые 30 минут)     | Метаданные Airflow → `stg_airflow` → системные витрины (`cdm.dm_sys_*`)  |

Все витрины пересобираются целиком при каждом запуске (`DELETE` + `INSERT`), поле `load_date` - время сборки.

### `sn_dwh_load`

```
create_stg_tables → load_stg_people / load_stg_groups / load_stg_messages
  → create_dds_tables → load_dds_hubs → load_dds_links / load_dds_satellites
  → create_cdm_tables → build_dm_user_activity / build_dm_group_activity / build_dm_demography / build_dm_messages_daily
```

- **stg** ([01_stg_ddl.sql](sql/01_stg_ddl.sql)): `people`, `groups`, `messages` - копии CSV из `data/social_network`,
  перезагружаются полностью. Загрузка падает, если заголовок CSV не совпал с ожидаемым или число строк
  в таблице не равно числу строк в файле; тогда остаются прежние данные.
- **dds** ([02_dds_ddl.sql](sql/02_dds_ddl.sql)): Data Vault, загружается инкрементально.
  Хабы `h_people`, `h_groups`, `h_chats` и линки `l_chat_owners`, `l_person_chat`, `l_groups` получают только
  новые ключи (`hash()` от бизнес-ключей). Сателлиты `s_person_info`, `s_group_info`, `s_chat_info` получают новую
  версию строки, только если изменился `hashdiff` - хеш всех атрибутов, так что история изменений сохраняется.
- **cdm**: витрины ниже строятся из представлений `cdm.v_people`, `cdm.v_groups`, `cdm.v_messages`
  ([06_cdm_ddl.sql](sql/06_cdm_ddl.sql)) - хабы, соединённые с последними версиями сателлитов.

Отчётная дата (`report_date`) всех аналитических витрин - дата последнего сообщения в данных.
Личное сообщение - то, у которого заполнен получатель; групповое - то, у которого заполнена группа.

#### `cdm.dm_user_activity` - активность пользователя

Одна строка на пользователя, включая тех, кто ничего не писал ([07_dm_user_activity.sql](sql/07_dm_user_activity.sql)).

| Поле                                       | Что означает                                                         |
|--------------------------------------------|----------------------------------------------------------------------|
| `person_id`, `name`, `country`             | Пользователь                                                         |
| `messages_sent`                            | Отправлено сообщений всего                                           |
| `private_sent`, `group_sent`               | Из них личных и в группы                                             |
| `messages_received`                        | Получено личных сообщений (у группового сообщения нет получателя)    |
| `first_message_date`, `last_message_date`  | Даты первого и последнего отправленного сообщения                    |
| `active_days`                              | Число разных дней, в которые пользователь писал                      |
| `groups_written_to`                        | В сколько разных групп писал                                         |
| `groups_owned`                             | Сколькими группами владеет                                           |
| `report_date`, `load_date`                 | Отчётная дата, время сборки                                          |

#### `cdm.dm_group_activity` - активность группы

Одна строка на группу ([08_dm_group_activity.sql](sql/08_dm_group_activity.sql)).

| Поле                                       | Что означает                                                                    |
|--------------------------------------------|---------------------------------------------------------------------------------|
| `group_id`, `title`, `status`              | Группа                                                                          |
| `owner_id`, `owner_name`                   | Владелец                                                                        |
| `created_date`, `group_age_days`           | Дата создания и возраст группы в днях на отчётную дату                          |
| `messages_total`                           | Сообщений в группе                                                              |
| `distinct_authors`                         | Разных авторов (модель не хранит участников, участник - тот, кто писал)         |
| `first_message_date`, `last_message_date`  | Первое и последнее сообщение                                                    |
| `messages_per_day`                         | Сообщений в день, считая от первого сообщения до отчётной даты                  |
| `days_silent`                              | Дней без сообщений: от последнего сообщения (или от создания) до отчётной даты  |
| `is_dead`                                  | «Мёртвая» группа: `days_silent` не меньше 21 (`DEAD_GROUP_DAYS` в DAG)          |
| `report_date`, `load_date`                 | Отчётная дата, время сборки                                                     |

#### `cdm.dm_demography` - демография

Одна строка на пару страна × возрастная группа ([09_dm_demography.sql](sql/09_dm_demography.sql)).

| Поле                       | Что означает                                                                          |
|----------------------------|---------------------------------------------------------------------------------------|
| `country`                  | Страна, `unknown` если не указана                                                     |
| `age_bucket`               | Возраст на отчётную дату: `<18`, `18-24`, `25-34`, `35-44`, `45-54`, `55+`, `unknown` |
| `users_total`              | Пользователей в группе                                                                |
| `users_active`             | Из них отправивших хотя бы одно сообщение                                             |
| `messages_total`           | Сообщений, отправленных этими пользователями                                          |
| `avg_messages_per_user`    | Среднее число сообщений на пользователя (по всем, включая неактивных)                 |
| `report_date`, `load_date` | Отчётная дата, время сборки                                                           |

#### `cdm.dm_messages_daily` - сообщения по дням

Одна строка на день, в который были сообщения; дни без сообщений не попадают
([10_dm_messages_daily.sql](sql/10_dm_messages_daily.sql)).

| Поле                                 | Что означает                                                    |
|--------------------------------------|-----------------------------------------------------------------|
| `msg_date`                           | День                                                            |
| `weekday_num`, `weekday_name`        | День недели: номер ISO (1 - понедельник) и сокращение (`Mon`)   |
| `is_weekend`                         | Суббота или воскресенье                                         |
| `messages_total`                     | Сообщений за день                                               |
| `private_messages`, `group_messages` | Из них личных и групповых                                       |
| `dau`                                | Разных авторов за день (daily active users)                     |
| `peak_hour`                          | Час с наибольшим числом сообщений, при равенстве - самый ранний |
| `load_date`                          | Время сборки                                                    |

### `sn_sys_marts`

```
create_stg_airflow_tables → extract_dag / extract_dag_run / extract_task_instance / extract_import_error / load_expected_schedules
  → check_schedules → build_dm_sys_schedules
  → build_dm_sys_task_runs
create_cdm_sys_tables → build_dm_sys_schedules / build_dm_sys_task_runs
```

- **extract_\*** копируют таблицы метаданных Airflow `dag`, `dag_run`, `task_instance`, `import_error`
  из PostgreSQL (подключение `airflow_metadata`) в одноимённые таблицы `stg_airflow`, время в UTC
  ([11_stg_airflow_ddl.sql](sql/11_stg_airflow_ddl.sql)).
- **load_expected_schedules** загружает эталонный список DAG, которые обязаны быть зарегистрированы,
  из [data/airflow/expected_schedules.csv](data/airflow/expected_schedules.csv): `dag_id`, ожидаемое расписание, имя файла.
- **check_schedules** на Python с `croniter` проверяет CRON-выражение каждого DAG и сравнивает запуски, которые
  должны были быть по CRON за последние 7 дней, с фактическими плановыми запусками. Последний момент по CRON первые
  5 минут не считается пропущенным: Airflow создаёт запуск с небольшой задержкой. Результат - `stg_airflow.dag_cron_check`
  (`cron_valid`, `missed_runs`), ожидаемое и фактическое число запусков пишется в лог задачи.

Снимок метаданных снимается во время работы самого `sn_sys_marts`, поэтому его текущий запуск
и его задачи видны в витринах как `running`.

#### `cdm.dm_sys_schedules` - корректность расписаний

Одна строка на DAG, который зарегистрирован в Airflow или есть в эталонном списке
([13_dm_sys_schedules.sql](sql/13_dm_sys_schedules.sql)). Проблемы видны по флагам:
`expected_not_registered`, `schedule_mismatch`, `cron_valid = false`, непустой `import_error`, `missed_runs > 0`.

| Поле                              | Что означает                                                                          |
|-----------------------------------|---------------------------------------------------------------------------------------|
| `dag_id`                          | DAG                                                                                   |
| `schedule_interval`               | Фактическое расписание в Airflow                                                      |
| `expected_schedule`               | Расписание из эталонного списка                                                       |
| `cron_valid`                      | Корректно ли CRON-выражение фактического расписания                                   |
| `expected_not_registered`         | В эталоне есть, в Airflow нет: файл удалён, не импортировался или DAG переименован    |
| `schedule_mismatch`               | Фактическое расписание отличается от эталонного                                       |
| `is_paused`                       | DAG на паузе                                                                          |
| `import_error`                    | Последняя строка ошибки импорта файла DAG, сопоставляется по имени файла              |
| `missed_runs`                     | Пропущенных плановых запусков за последние 7 дней                                     |
| `last_run_at`, `last_run_state`   | Время и статус последнего запуска (любого типа)                                       |
| `load_date`                       | Время сборки                                                                          |

#### `cdm.dm_sys_task_runs` - статусы задач

Одна строка на пару `dag_id` × `task_id` ([14_dm_sys_task_runs.sql](sql/14_dm_sys_task_runs.sql)).

| Поле                             | Что означает                                                             |
|----------------------------------|--------------------------------------------------------------------------|
| `dag_id`, `task_id`              | Задача                                                                   |
| `last_state`, `last_run_at`      | Статус и время последнего начатого запуска                               |
| `runs_total`                     | Всего запусков задачи                                                    |
| `success_count`, `failed_count`  | Успешных и упавших                                                       |
| `success_rate_pct`               | Процент успеха: success / (success + failed)                             |
| `avg_duration_sec`               | Средняя длительность завершённых запусков, секунды                       |
| `retries_total`                  | Всего повторов после неудачных попыток                                   |
| `is_stuck`                       | «Зависла»: выполняется 10 минут и дольше (`STUCK_AFTER_MINUTES` в DAG)    |
| `load_date`                      | Время сборки                                                             |
