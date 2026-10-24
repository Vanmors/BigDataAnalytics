# BigDataAnalytics

Лабораторная на Airflow, PostgreSQL и MongoDB: месячная витрина, загрузка CSV,
SQL-аудит и история изменений MongoDB.

Нужны Docker с Compose для Linux-контейнеров, интернет и свободный порт 8080. Из корня
скачанного репозитория выполните:

```sh
docker compose -f compose.demo.yml run --build --rm demo
```

Команда готовит отдельный учебный стенд, выполняет три DAG и проверяет результаты.
После `PASS` откройте [Airflow](http://127.0.0.1:8080), логин и пароль — `airflow`.
Python, make и `.env` на компьютере не нужны.

- [Что показать и как остановить стенд](docs/demo.md).
- [Архитектура](docs/task3/architecture.md), [восстановление Mongo CDC](docs/task3/operations.md).
- [Тесты](docs/task3/testing.md), [исправления учебных данных](docs/task3/fixtures.md).
- [Оригинальные условия и примеры](docs/assignments/README.md).

Стенд предназначен для локального показа с учебными данными. Данные и история
остаются в томах проекта `bigdataanalytics-demo`; повтор команды не удаляет их.
