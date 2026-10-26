"""Run the dedicated, persistent teaching demo; never reset existing data."""
import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import runpy
import subprocess
import sys

ROOT = Path('/opt/airflow')
DEMO_LOCK = (20261024, 1)
DAGS = {
    'task1_customer_report_mart': 'task1_customer_report_mart.py',
    'task2_external_source_ingest': 'task2_external_source_ingest.py',
    'task3_mongo_cdc': 'task3_mongo_cdc.py',
}
SOURCE_TABLES = ('craft_market_craftsmans', 'craft_market_customers', 'craft_market_orders')
EXTERNAL_TABLES = ('user_order_log', 'user_activity_log', 'customer_research')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def command(*args):
    subprocess.run(args, check=True)


def initialize():
    command('airflow', 'db', 'migrate')
    from airflow.www.app import create_app
    with create_app().app_context():
        from flask import current_app
        user = current_app.appbuilder.sm.find_user(username='airflow')
        if user is None:
            command('airflow', 'users', 'create', '--username', 'airflow', '--password', 'airflow',
                    '--firstname', 'Lab', '--lastname', 'Demo', '--role', 'Admin',
                    '--email', 'airflow@example.invalid')
        else:
            from werkzeug.security import check_password_hash
            if not check_password_hash(user.password, 'airflow'):
                command('airflow', 'users', 'reset-password', '--username', 'airflow', '--password', 'airflow')
            if 'Admin' not in {role.name for role in user.roles}:
                command('airflow', 'users', 'add-role', '--username', 'airflow', '--role', 'Admin')
    runpy.run_path(str(ROOT / 'data/generate_data.py'), run_name='__main__')
    print('PASS: Airflow и учебные CSV готовы.', flush=True)


@contextmanager
def demo_lock(pg):
    pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute('SELECT pg_try_advisory_lock(%s, %s)', DEMO_LOCK)
        owned = cur.fetchone()[0]
    require(owned, 'Демонстрация уже запущена. Дождитесь её завершения.')
    try:
        yield
    finally:
        if not pg.closed:
            with pg.cursor() as cur:
                cur.execute('SELECT pg_advisory_unlock(%s, %s)', DEMO_LOCK)


def check_source(state, db, cdc):
    require(not db.client.admin.command('currentOp').get('fsyncLock', False),
            'MongoDB уже заблокирована. Автоматическая разблокировка запрещена; проверьте предыдущий baseline.')
    require(state['status'] in {'uninitialized', 'active'},
            'CDC требует ручного восстановления: needs_manual_rebaseline. История сохранена; повторный baseline не запущен.')
    cdc.validate_binding(state, cdc.identity(db))


def publish_baseline(admin, publish):
    require(not admin.command('currentOp').get('fsyncLock', False),
            'MongoDB уже заблокирована; baseline не запущен.')
    try:
        result = admin.command({'fsync': 1, 'lock': True})
        require(result.get('lockCount') == 1, 'Не удалось подтвердить единственную блокировку MongoDB.')
        result = publish()
        require(result.get('status') == 'baseline_published', 'Публикация baseline не подтверждена.')
    except BaseException:
        print('ОШИБКА: исход baseline не подтверждён. Блокировка MongoDB сохранена; '
              'не повторяйте baseline и не снимайте блокировку до проверки task3.mongo_state и журнала ошибки.',
              file=sys.stderr, flush=True)
        raise
    require(admin.command('fsyncUnlock').get('lockCount') == 0,
            'Baseline опубликован, но MongoDB остаётся заблокирована; требуется проверка оператора.')
    return result


def execute_dag(dag_id):
    # Different January timestamps preserve previous demonstration runs and the task1 month.
    logical_date = datetime.now(timezone.utc).replace(year=2024, month=1, day=1)
    command('airflow', 'dags', 'test', dag_id, logical_date.isoformat(),
            '--subdir', str(ROOT / 'dags' / DAGS[dag_id]))
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    with create_session() as session:
        saved = session.query(DagRun).filter(DagRun.dag_id == dag_id,
                                            DagRun.execution_date == logical_date).one()
        require(saved.state == 'success', f'DAG {dag_id} завершился со статусом {saved.state}.')


def verify_task1(hook):
    actual = hook.get_first('''SELECT count(*), sum(count_order), sum(customer_spent), sum(platform_money)
        FROM mart.craft_market_customer_report WHERE report_period='2024-01' ''')
    require(actual == (195, 704, Decimal('8802285.00'), Decimal('880228.50')),
            f'Задание 1: результат отличается от независимого эталона: {actual}. Исходные строки сохранены.')
    print('PASS: задание 1 — 195 заказчиков, 704 заказа; суммы 8802285.00 и 880228.50.', flush=True)


def external_snapshot(hook):
    result = {}
    for table in EXTERNAL_TABLES:
        rows = hook.get_records(f'SELECT to_jsonb(t) FROM external_source.{table} t ORDER BY to_jsonb(t)::text')
        require(len(rows) == 10, f'Задание 2: external_source.{table} содержит {len(rows)} строк вместо 10.')
        result[table] = rows
    return result


def verify_sql_audit(pg):
    from uuid import uuid4
    key = -int(uuid4().hex[:15], 16)
    with pg.cursor() as cur:
        cur.execute('SELECT count(*), coalesce(max(id), 0) FROM task3.logs')
        count, last_id = cur.fetchone()
    pg.autocommit = False
    try:
        with pg.cursor() as cur:
            cur.execute('INSERT INTO source3.craft_market_craftsmans(craftsman_id,craftsman_name) VALUES (%s,%s)',
                        (key, 'demo before'))
            cur.execute('UPDATE source3.craft_market_craftsmans SET craftsman_name=%s WHERE craftsman_id=%s',
                        ('demo after', key))
            cur.execute('DELETE FROM source3.craft_market_craftsmans WHERE craftsman_id=%s', (key,))
            cur.execute('TRUNCATE source1.craft_market_wide')
            cur.execute('''SELECT table_schema,table_name,operation,old_data,new_data
                           FROM task3.logs WHERE id>%s ORDER BY id''', (last_id,))
            rows = cur.fetchall()
            require([row[:3] for row in rows] == [
                ('source3', 'craft_market_craftsmans', 'INSERT'),
                ('source3', 'craft_market_craftsmans', 'UPDATE'),
                ('source3', 'craft_market_craftsmans', 'DELETE'),
                ('source1', 'craft_market_wide', 'TRUNCATE')], 'SQL-аудит не записал все четыре операции.')
            require(rows[0][3] is None and rows[0][4]['craftsman_name'] == 'demo before'
                    and rows[1][3]['craftsman_name'] == 'demo before'
                    and rows[1][4]['craftsman_name'] == 'demo after'
                    and rows[2][3]['craftsman_name'] == 'demo after' and rows[2][4] is None
                    and rows[3][3:] == (None, None), 'SQL-аудит сохранил неверные old_data/new_data.')
    finally:
        pg.rollback()
        pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute('SELECT count(*) FROM task3.logs')
        require(cur.fetchone()[0] == count, 'Проверочная транзакция SQL-аудита не откачена.')
    print('PASS: SQL-аудит — INSERT, UPDATE, DELETE, TRUNCATE и old_data/new_data; проверочные изменения отменены.',
          flush=True)


def verify_mongo(hook, db, cdc):
    from bson import ObjectId, json_util
    from bson.raw_bson import RawBSONDocument
    from pymongo.write_concern import WriteConcern
    key = ObjectId()
    clients = db.clients.with_options(write_concern=WriteConcern('majority', wtimeout=5000))
    clients.insert_one({'_id': key, 'name': 'demo before', 'nested': {'values': [1, 2]}})
    clients.update_one({'_id': key}, {'$set': {'name': 'demo after'}})
    clients.delete_one({'_id': key})
    execute_dag('task3_mongo_cdc')
    rows = hook.get_records('''SELECT operation,raw_bson,document,projection_status
        FROM task3.mongo_events WHERE kind='change' AND collection='clients' AND document_id=%s ORDER BY id''',
                            parameters=(str(key),))
    require([row[0] for row in rows] == ['insert', 'update', 'delete'],
            'Mongo CDC не сохранил ровно три проверочных события insert/update/delete.')
    for row, expected_name in zip(rows, ('demo before', 'demo after', 'demo after')):
        operation, raw, document, status = row
        event = RawBSONDocument(bytes(raw), cdc.RAW_CODEC)
        selected = event['fullDocumentBeforeChange' if operation == 'delete' else 'fullDocument']
        require(event['operationType'] == operation and event['documentKey']['_id'] == key
                and selected['_id'] == key and selected['name'] == expected_name
                and selected['nested']['values'] == [1, 2], 'Mongo CDC сохранил неверный полный BSON-документ.')
        expected = json.loads(json_util.dumps(selected, json_options=json_util.RELAXED_JSON_OPTIONS))
        require(status == 'available' and document == expected,
                'JSON-проекция Mongo CDC не соответствует полному BSON-документу.')
    require(hook.get_first('SELECT count(*) FROM task3.mongo_latest WHERE collection=%s AND document_id=%s',
                           parameters=('clients', str(key)))[0] == 0,
            'Удалённый документ остался в task3.mongo_latest.')
    before = hook.get_first('SELECT count(*) FROM task3.mongo_events')[0]
    execute_dag('task3_mongo_cdc')
    require(hook.get_first('SELECT count(*) FROM task3.mongo_events')[0] == before,
            'Повторный запуск Mongo CDC добавил неожиданные события.')
    require(hook.get_first('SELECT status FROM task3.mongo_state WHERE singleton')[0] == 'active',
            'Mongo CDC требует ручного восстановления; история сохранена.')
    print('PASS: Mongo CDC — три полных BSON-события, верные JSON-проекции, удаление из latest; повтор без дублей.',
          flush=True)


def demonstrate():
    from airflow.hooks.base import BaseHook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import task3_mongo_cdc as cdc
    hook = PostgresHook(postgres_conn_id='dwh_postgres')
    mongo = BaseHook.get_connection('task3_mongo')
    with closing(hook.get_conn()) as pg, cdc.mongo_client(mongo.get_uri()) as client, demo_lock(pg):
        db = client[mongo.schema]
        require(not client.admin.command('currentOp').get('fsyncLock', False),
                'MongoDB уже заблокирована. Проверьте предыдущий baseline; автоматическая разблокировка запрещена.')
        with pg.cursor() as cur:
            cur.execute((ROOT / 'sql/01_ddl_craft_market.sql').read_text())
            cur.execute((ROOT / 'sql/05_task3.sql').read_text())
        state = cdc.state_row(pg)
        check_source(state, db, cdc)
        with pg.cursor() as cur:
            counts = []
            for table in SOURCE_TABLES:
                cur.execute(f'SELECT count(*) FROM source3.{table}')
                counts.append(cur.fetchone()[0])
        if not any(counts):
            hook.run((ROOT / 'sql/04_seed_craft_market.sql').read_text(), autocommit=False)
        if state['status'] == 'uninitialized':
            runpy.run_path(str(ROOT / 'mongo/task3-seed.py'), run_name='__main__')
            publish_baseline(client.admin, lambda: cdc.airflow_run('baseline', pause_held=True))
            print('PASS: первый baseline опубликован; собственная блокировка MongoDB снята.', flush=True)
        else:
            print('PASS: CDC уже активен; checkpoint и история сохранены.', flush=True)
        execute_dag('task1_customer_report_mart')
        verify_task1(hook)
        execute_dag('task2_external_source_ingest')
        first = external_snapshot(hook)
        execute_dag('task2_external_source_ingest')
        require(external_snapshot(hook) == first, 'Повтор задания 2 изменил содержимое таблиц.')
        print('PASS: задание 2 — по 10 строк в трёх таблицах; повтор без дублей.', flush=True)
        verify_sql_audit(pg)
        verify_mongo(hook, db, cdc)
    print('\nPASS: все три задания проверены.\nAirflow: http://127.0.0.1:8080 (airflow / airflow)', flush=True)
    for dag in DAGS:
        print(f'http://127.0.0.1:8080/dags/{dag}/grid', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('init', 'run'))
    args = parser.parse_args()
    try:
        initialize() if args.action == 'init' else demonstrate()
    except Exception as exc:
        print(f'ОШИБКА: {exc}', file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
