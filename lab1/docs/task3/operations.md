# Остановка и восстановление

Для обычного показа достаточно [одной команды](../demo.md). Она работает только
с отдельным учебным проектом `bigdataanalytics-demo`: начальный снимок выполняет
при `uninitialized`, при `active` продолжает поток, при `needs_manual_rebaseline`
останавливается. Повтор не удаляет историю и не меняет привязку источника.

UI доступен на `127.0.0.1:8080`; базы не имеют портов на хосте. MongoDB без
аутентификации находится в доверенной сети Compose. Стенд не предназначен
для общих серверов и реальных данных. Остановка с сохранением томов:

```sh
docker compose -f compose.demo.yml stop
```

## Ручное восстановление Mongo CDC

Эта процедура нужна при `needs_manual_rebaseline`. Она восстанавливает текущее
состояние, сохраняет старую историю и добавляет один `gap`; утраченные промежуточные
версии вернуть нельзя. Допустимый срок простоя CDC не гарантируется.
`baseline` разрешён также для первого снимка из `uninitialized`, но отклоняет `active`.

1. Приостановите `task3_mongo_cdc` в [Airflow](http://127.0.0.1:8080/dags/task3_mongo_cdc/grid),
   дождитесь окончания задачи и остановите всех писателей MongoDB. Завершите подготовку
   источника до блокировки. Оставьте отдельный терминал для unlock. Состояние можно проверить:

   ```sh
   docker compose -f compose.demo.yml exec postgres-dwh psql -U dwh -d dwh -c 'SELECT status, epoch, failure FROM task3.mongo_state;'
   ```

2. Проверьте отсутствие чужой блокировки (`fsyncLock` отсутствует или `false`),
   затем приобретите свою **один раз**. Сохраните ответ с `lockCount:1`:

   ```sh
   docker compose -f compose.demo.yml exec mongo mongosh --quiet --eval 'db.adminCommand({currentOp:1}).fsyncLock'
   docker compose -f compose.demo.yml exec mongo mongosh --quiet --eval 'db.adminCommand({fsync:1,lock:true,fsyncLockAcquisitionTimeoutMillis:5000})'
   ```

3. Непрерывно удерживая эту блокировку, выполните baseline. `pause_held=True` —
   утверждение оператора о непрерывном владении; загрузчик блокировку не приобретает:

   ```sh
   docker compose -f compose.demo.yml run --rm --no-deps --entrypoint python demo -c "from task3_mongo_cdc import airflow_run; airflow_run('baseline', pause_held=True)"
   ```

4. Сохраните ответ `baseline_published`, эпоху и checkpoint. Только после подтверждения
   публикации снимите **свою** блокировку, проверьте `lockCount:0` и отсутствие `fsyncLock`.
   Затем возобновите писателей и DAG:

   ```sh
   docker compose -f compose.demo.yml exec mongo mongosh --quiet --eval 'db.adminCommand({fsyncUnlock:1})'
   docker compose -f compose.demo.yml exec mongo mongosh --quiet --eval 'db.adminCommand({currentOp:1}).fsyncLock'
   ```

Известный unlock/relock, перезапуск или изменение источника во время снимка требуют
отмены: две проверки блокировки не доказывают её непрерывность. Ошибка до commit
откатывает весь снимок; блокировка остаётся ответственностью оператора. При
`PublicationUnknown` проверьте `mongo_state` и исход публикации до повтора или unlock.
Не повторяйте команды вслепую и не уменьшайте чужой lock count.

`active` не означает свежесть, `empty_at_read` означает пустое чтение в тот момент,
`budget_reached` требует следующего consume; `contended` — другой загрузчик уже работает.
Ошибки UUID, настроек полных образов, отсутствующей коллекции или неподдерживаемого `_id`
не исправляются сбросом или сменой источника. Несовместимая структура требует миграции.
Не удаляйте тома или историю. SQL для просмотра — `sql/06_task3_checks.sql`;
точные данные хранятся в BSON, JSONB используется для запросов.
