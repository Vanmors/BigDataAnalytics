"""Acceptance checks against isolated teaching databases; no existing data reset."""
import csv
import importlib.util
import io
import os
import time
import subprocess
import sys
import tempfile
import unittest
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection, make_dsn, parse_dsn
from bson import Binary, Decimal128, Int64, ObjectId, Timestamp, json_util
from bson.raw_bson import RawBSONDocument
from pymongo.errors import OperationFailure
import task3_mongo_cdc as cdc

ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.environ.get('TASK3_PG_DSN'), 'requires isolated tests/compose.yml resources')
class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.name = 'test_' + uuid.uuid4().hex
        config = parse_dsn(os.environ['TASK3_PG_DSN'])
        admin = psycopg2.connect(**config)
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.name)))
        admin.close()
        config['dbname'] = self.name
        self.factory = lambda: psycopg2.connect(**config, connect_timeout=5)
        self.pg = self.factory()
        self.pg.autocommit = True
        with self.pg.cursor() as cur:
            cur.execute((ROOT / 'sql/01_ddl_craft_market.sql').read_text())
            cur.execute((ROOT / 'sql/05_task3.sql').read_text())
        self.client = cdc.mongo_client(os.environ['TASK3_MONGO_URI'])
        self.db = self.client[self.name]
        for name in cdc.COLLECTIONS:
            self.db.create_collection(name, changeStreamPreAndPostImages={'enabled': True})
        self.raw = self.db.with_options(codec_options=cdc.RAW_CODEC)
        self.addCleanup(self.pg.close)
        self.addCleanup(self.client.close)
        # Test databases intentionally remain in dedicated test volumes for inspection.

    def rows(self, query, params=None):
        with self.pg.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def run_capture(self, command='consume', **kwargs):
        return cdc.run(self.pg, self.client, self.name, command, pg_factory=self.factory, **kwargs)

    def baseline(self, **kwargs):
        self.assertFalse(self.client.admin.command('currentOp').get('fsyncLock', False))
        self.assertEqual(self.client.admin.command({'fsync': 1, 'lock': True})['lockCount'], 1)
        try:
            return self.run_capture('baseline', pause_held=True, **kwargs)
        finally:
            # The test operator acquired exactly this lock; the loader never releases it.
            if self.client.admin.command('currentOp').get('fsyncLock', False):
                self.assertEqual(self.client.admin.command('fsyncUnlock')['lockCount'], 0)

    def snapshot(self):
        return tuple(self.rows('SELECT * FROM task3.' + name + ' ORDER BY 1')
                     for name in ('mongo_state', 'mongo_events', 'mongo_latest'))

    def test_empty_initialization_and_repeated_baseline_rejected(self):
        self.assertEqual(self.run_capture()['status'], 'manual_baseline_required')
        self.assertEqual(self.baseline()['documents'], 0)
        self.assertEqual(self.rows("SELECT count(*) FROM task3.mongo_events WHERE kind='gap'")[0][0], 0)
        before = self.snapshot()
        with self.assertRaises(cdc.ContractError):
            self.run_capture('baseline', pause_held=True)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.run_capture()['status'], 'empty_at_read')
        saved = bytes(self.rows('SELECT resume_token FROM task3.mongo_state')[0][0])
        self.db.clients.insert_one({'_id': ObjectId(), 'name': 'first post-unlock'})
        self.assertEqual(self.run_capture()['events_committed'], 1)
        self.assertNotEqual(bytes(self.rows('SELECT resume_token FROM task3.mongo_state')[0][0]), saved)

    def test_raw_versions_projection_tokens_and_delete(self):
        key = ObjectId()
        doc = {'_id': key, 'name': 'first', 'nested': {'z': 1, 'a': [1, 2]},
               'int64': Int64(42), 'decimal': Decimal128('1.25'), 'binary': Binary(b'\x00\x02', 128),
               'date': datetime(2024, 1, 1, tzinfo=timezone.utc), 'stamp': Timestamp(12, 3),
               'wrapper': {'$oid': 'user-shaped-wrapper'}}
        self.db.clients.insert_one(doc)
        original = self.raw.clients.find_one({'_id': key}).raw
        self.baseline()
        self.assertEqual(bytes(self.rows("SELECT raw_bson FROM task3.mongo_events WHERE kind='baseline'")[0][0]), original)
        state = cdc.state_row(self.pg)
        expected = []
        source_images = [original]
        with cdc.watch(self.raw, state['resume_token']) as witness:
            for value in ('second', 'third'):
                self.db.clients.update_one({'_id': key}, {'$set': {'name': value}})
                source_images.append(self.raw.clients.find_one({'_id': key}).raw)
            self.db.clients.replace_one({'_id': key}, {'_id': key, 'name': 'replacement'})
            source_images.append(self.raw.clients.find_one({'_id': key}).raw)
            self.db.clients.delete_one({'_id': key})
            deadline = time.monotonic() + 5
            while len(expected) < 4 and time.monotonic() < deadline:
                event = witness.try_next()
                if event:
                    expected.append(event)
            self.assertEqual(len(expected), 4)
        self.assertEqual(self.run_capture()['events_committed'], 4)
        stored = self.rows("SELECT raw_bson,token,document FROM task3.mongo_events WHERE kind='change' ORDER BY id")
        for index, ((raw, token, projected), event) in enumerate(zip(stored, expected)):
            self.assertEqual(bytes(raw), event.raw)
            self.assertEqual(bytes(token), event['_id'].raw)
            actual = RawBSONDocument(bytes(raw), cdc.RAW_CODEC)
            self.assertEqual(actual['fullDocumentBeforeChange'].raw, source_images[index])
            if index < 3:
                self.assertEqual(actual['fullDocument'].raw, source_images[index + 1])
            self.assertEqual(projected['name'], ('second', 'third', 'replacement', 'replacement')[index])
        self.assertEqual(self.rows('SELECT count(*) FROM task3.mongo_latest')[0][0], 0)
        self.assertEqual(self.run_capture()['events_committed'], 0)

    def test_projection_unavailable_preserves_bson(self):
        key = self.db.clients.insert_one({'_id': ObjectId(), 'name': 'nul\x00value'}).inserted_id
        original = self.raw.clients.find_one({'_id': key}).raw
        self.assertEqual(self.baseline()['unavailable_projections'], 1)
        for table in ('mongo_events', 'mongo_latest'):
            raw, doc, status, reason = self.rows('SELECT raw_bson,document,projection_status,projection_reason FROM task3.' + table + " WHERE collection='clients'")[0]
            self.assertEqual(bytes(raw), original)
            self.assertIsNone(doc)
            self.assertEqual(status, 'unavailable')
            self.assertEqual(reason, 'jsonb_sqlstate_22P05')
        self.db.clients.update_one({'_id': key}, {'$set': {'name': 'valid'}})
        self.assertEqual(self.run_capture()['events_committed'], 1)
        self.assertEqual(self.rows('SELECT document->>\'name\' FROM task3.mongo_latest')[0][0], 'valid')

    def test_late_baseline_rollback_visibility_and_limits(self):
        for collection in cdc.COLLECTIONS:
            self.db[collection].insert_many([{'_id': ObjectId(), 'name': collection} for _ in range(61)])
        before = self.snapshot()
        original = cdc.latest
        calls = []
        observer = self.factory()
        observer.autocommit = True
        try:
            def late_failure(*args):
                original(*args)
                calls.append(1)
                with observer.cursor() as cur:
                    cur.execute('SELECT count(*) FROM task3.mongo_latest')
                    self.assertEqual(cur.fetchone()[0], 0)
                if len(calls) == 183:
                    raise RuntimeError('injected final-page failure')
            with patch.object(cdc, 'latest', side_effect=late_failure):
                with self.assertRaisesRegex(RuntimeError, 'final-page'):
                    self.baseline()
            self.assertEqual(self.snapshot(), before)
            for setting in ('BASELINE_MAX_DOCUMENTS', 'BASELINE_MAX_BYTES', 'BASELINE_SECONDS'):
                with patch.object(cdc, setting, 0):
                    with self.assertRaises(cdc.ContractError):
                        self.baseline()
                self.assertEqual(self.snapshot(), before)
        finally:
            observer.close()
        self.assertEqual(self.baseline()['documents'], 183)

    def test_lock_contention_and_lock_loss(self):
        other = self.factory()
        other.autocommit = True
        try:
            with cdc.store_lock(other):
                self.assertEqual(self.run_capture(), {'status': 'contended'})
                self.assertEqual(self.run_capture('baseline', pause_held=True), {'status': 'contended'})
        finally:
            other.close()
        before = self.snapshot()
        self.db.clients.insert_one({'_id': ObjectId()})
        original = cdc.latest
        def lose_lock(*args):
            original(*args)
            self.assertEqual(self.client.admin.command('fsyncUnlock')['lockCount'], 0)
        with patch.object(cdc, 'latest', side_effect=lose_lock):
            with self.assertRaises(cdc.ContractError):
                self.baseline()
        self.assertEqual(self.snapshot(), before)

    def test_duplicates_transactions_event_budget_and_commit_rollback(self):
        self.baseline()
        key = ObjectId()
        for name in cdc.COLLECTIONS:
            self.db[name].insert_one({'_id': key, 'value': 0})
        self.run_capture()
        old_state = cdc.state_row(self.pg)
        with cdc.watch(self.raw, old_state['resume_token']) as witness:
            with self.client.start_session() as session:
                with session.start_transaction():
                    for name in cdc.COLLECTIONS:
                        self.db[name].update_one({'_id': key}, {'$set': {'value': 1}}, session=session)
            duplicate = witness.try_next()
            while duplicate is None:
                duplicate = witness.try_next()
        with patch.object(cdc, 'RUN_EVENTS', 1), patch.object(cdc, 'BATCH_EVENTS', 1):
            result = self.run_capture()
            self.assertEqual(result['status'], 'budget_reached')
            self.assertEqual(result['events_committed'], 1)
        self.run_capture()
        self.db.clients.update_one({'_id': key}, {'$set': {'value': 2}})
        self.run_capture()
        before = self.snapshot()
        with cdc.store_lock(self.pg):
            n, _ = cdc.apply_batch(self.pg, self.raw, cdc.state_row(self.pg), [cdc.prepare_event(duplicate)],
                                   bytes(cdc.state_row(self.pg)['resume_token']))
        self.assertEqual(n, 0)
        self.assertEqual(self.rows("SELECT document->>'value' FROM task3.mongo_latest WHERE collection='clients'")[0][0], '2')
        self.assertEqual(len(self.rows("SELECT id FROM task3.mongo_events WHERE kind='change'")), 7)
        self.db.clients.update_one({'_id': key}, {'$set': {'value': 3}})
        durable = self.snapshot()
        original = cdc.latest
        def before_commit(*args):
            original(*args)
            raise RuntimeError('before commit')
        with patch.object(cdc, 'latest', side_effect=before_commit):
            with self.assertRaises(RuntimeError):
                self.run_capture()
        self.assertEqual(self.snapshot(), durable)
        self.assertEqual(self.run_capture()['events_committed'], 1)
        self.assertEqual(self.run_capture()['events_committed'], 0)
        with patch.object(cdc, 'READ_SECONDS', 0):
            self.assertEqual(self.run_capture()['status'], 'budget_reached')

    def test_operator_aborts_known_unlock_relock(self):
        self.db.clients.insert_one({'_id': ObjectId()})
        before = self.snapshot()
        with self.assertRaises(cdc.ContractError):
            self.run_capture('baseline')
        original = cdc.latest
        def interrupted(*args):
            original(*args)
            self.assertEqual(self.client.admin.command('fsyncUnlock')['lockCount'], 0)
            self.assertEqual(self.client.admin.command({'fsync': 1, 'lock': True})['lockCount'], 1)
            # The operator knows the pause was broken and must abort the attempt.
            raise cdc.ContractError('operator reported unlock/relock; abort')
        with patch.object(cdc, 'latest', side_effect=interrupted):
            with self.assertRaises(cdc.ContractError):
                self.baseline()
        self.assertEqual(self.snapshot(), before)

    def test_baseline_ambiguous_commit_readback(self):
        self.db.clients.insert_one({'_id': ObjectId()})
        class LostAck(connection):
            def commit(self):
                super().commit()
                raise psycopg2.OperationalError('injected lost acknowledgement')
        config = parse_dsn(make_dsn(os.environ['TASK3_PG_DSN'], dbname=self.name))
        original = self.pg
        class FailedCommit(connection):
            def commit(self):
                raise psycopg2.OperationalError('injected failure before server commit')
        failed = psycopg2.connect(**config, connection_factory=FailedCommit)
        self.pg = failed
        try:
            with self.assertRaises(cdc.PublicationUnknown):
                self.baseline()
        finally:
            self.pg = original
            failed.close()
        self.assertEqual(cdc.state_row(self.pg)['status'], 'uninitialized')
        self.assertEqual(self.rows('SELECT count(*) FROM task3.mongo_events')[0][0], 0)
        special = psycopg2.connect(**config, connection_factory=LostAck)
        self.pg = special
        try:
            self.assertEqual(self.baseline()['status'], 'baseline_published')
            self.assertTrue(special.closed)
        finally:
            self.pg = original
            special.close()
        self.assertEqual(cdc.state_row(self.pg)['epoch'], 1)
        with self.assertRaises(cdc.ContractError):
            self.run_capture('baseline', pause_held=True)
        self.assertEqual(self.rows("SELECT count(*) FROM task3.mongo_events WHERE kind='epoch'")[0][0], 1)

    def test_sql_timeout_network_error_and_event_size_leave_checkpoint(self):
        key = self.db.clients.insert_one({'_id': ObjectId(), 'v': 0}).inserted_id
        self.baseline()
        self.db.clients.update_one({'_id': key}, {'$set': {'v': 1}})
        before = self.snapshot()
        with patch.object(cdc, 'MAX_BUFFER_BYTES', 1):
            with self.assertRaises(cdc.ContractError):
                self.run_capture()
        self.assertEqual(self.snapshot(), before)
        blocker = self.factory()
        try:
            with blocker.cursor() as cur:
                cur.execute('LOCK task3.mongo_latest IN ACCESS EXCLUSIVE MODE')
            with self.assertRaises(psycopg2.errors.LockNotAvailable):
                self.run_capture()
        finally:
            blocker.rollback()
            blocker.close()
        self.assertEqual(self.snapshot(), before)
        from pymongo.errors import ServerSelectionTimeoutError
        with cdc.mongo_client('mongodb://mongo:1/?directConnection=true') as unavailable:
            with self.assertRaises(ServerSelectionTimeoutError):
                cdc.run(self.pg, unavailable, self.name)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.run_capture()['events_committed'], 1)

    def test_hard_crash_before_and_after_commit(self):
        self.baseline()
        key = self.db.clients.insert_one({'_id': ObjectId(), 'v': 1}).inserted_id
        env = dict(os.environ, TASK3_PG_DSN=make_dsn(os.environ['TASK3_PG_DSN'], dbname=self.name),
                   TASK3_MONGO_DATABASE=self.name)
        before = self.snapshot()
        result = subprocess.run([sys.executable, str(ROOT / 'tests/crash_worker.py')],
                                env=dict(env, TASK3_CRASH_POINT='before'), timeout=20, check=False)
        self.assertEqual(result.returncode, 91)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.run_capture()['events_committed'], 1)
        self.db.clients.update_one({'_id': key}, {'$set': {'v': 2}})
        result = subprocess.run([sys.executable, str(ROOT / 'tests/crash_worker.py')],
                                env=dict(env, TASK3_CRASH_POINT='after'), timeout=20, check=False)
        self.assertEqual(result.returncode, 92)
        self.assertEqual(self.rows("SELECT document->>'v' FROM task3.mongo_latest")[0][0], '2')
        self.assertEqual(self.run_capture()['events_committed'], 0)
        self.assertEqual(self.rows("SELECT count(*) FROM task3.mongo_events WHERE kind='change'")[0][0], 2)

    def test_binding_options_identifier_and_lifecycle_failures(self):
        self.baseline()
        before = self.snapshot()
        self.db.command({'collMod': 'clients', 'changeStreamPreAndPostImages': {'enabled': False}})
        with self.assertRaises(cdc.ContractError):
            self.run_capture()
        self.assertEqual(self.snapshot(), before)
        self.db.command({'collMod': 'clients', 'changeStreamPreAndPostImages': {'enabled': True}})
        self.db.clients.insert_one({'_id': 42})
        with self.assertRaises(cdc.ContractError):
            self.run_capture()
        self.assertEqual(self.snapshot(), before)
        # Rename/drop must stay visible in the pipeline and fail explicitly.
        with self.assertRaises(cdc.ContractError):
            cdc.prepare_event(RawBSONDocument(__import__('bson').BSON.encode({'operationType': 'rename', 'ns': {'coll': 'other'}, 'to': {'db': self.name, 'coll': 'clients'}})))
        self.db.restaurant.rename('renamed')
        with self.assertRaises(cdc.ContractError):
            self.run_capture()
        self.assertEqual(self.snapshot(), before)
        self.db.create_collection('restaurant', changeStreamPreAndPostImages={'enabled': True})
        with self.assertRaisesRegex(cdc.ContractError, 'binding'):
            self.run_capture()
        self.assertEqual(self.snapshot(), before)

    def test_loss_stop_preserving_recovery_and_failed_recovery(self):
        self.db.clients.insert_one({'_id': ObjectId(), 'value': 1})
        self.baseline()
        old = self.snapshot()
        for code in (286, 47):
            # Classification injection; real independent expiration is a separate slow gate.
            with patch.object(cdc, 'watch', side_effect=OperationFailure('injected required pre-image was not found', code)):
                self.assertEqual(self.run_capture()['status'], 'needs_manual_rebaseline')
            self.assertEqual(bytes(cdc.state_row(self.pg)['resume_token']), bytes(old[0][0][6]))
            self.assertEqual(self.run_capture()['status'], 'needs_manual_rebaseline')
            stopped = self.snapshot()
            with patch.object(cdc, 'BASELINE_MAX_DOCUMENTS', 0):
                with self.assertRaises(cdc.ContractError):
                    self.baseline()
            self.assertEqual(self.snapshot(), stopped)
            self.baseline()
            self.assertEqual(self.rows("SELECT count(*) FROM task3.mongo_events WHERE kind='gap'")[0][0], 1 if code == 286 else 2)
            old = self.snapshot()
        self.assertEqual(self.rows("SELECT count(*) FROM task3.mongo_events WHERE kind='baseline'")[0][0], 3)

    def test_fixtures_sql_query_and_join(self):
        for name in cdc.COLLECTIONS:
            self.db[name].insert_many(json_util.loads((ROOT / f'data/task3/{name}.json').read_text()))
        self.baseline()
        self.assertEqual(self.rows("SELECT count(*) FROM task3.mongo_latest o JOIN task3.mongo_latest c ON c.collection='clients' AND c.document_id=o.document #>> '{client,id,$oid}' WHERE o.collection='orders'")[0][0], 1)
        self.assertEqual(self.rows("SELECT document->>'name' FROM task3.mongo_latest WHERE collection='restaurant'")[0][0], 'Булочная №13')

    def test_all_six_audit_targets_copy_images_and_truncate(self):
        targets = [
            ('source1', 'craft_market_wide'), ('source2', 'craft_market_masters_products'),
            ('source2', 'craft_market_orders_customers'), ('source3', 'craft_market_craftsmans'),
            ('source3', 'craft_market_customers'), ('source3', 'craft_market_orders')]
        keys = {}
        for schema, table in targets:
            columns = self.rows("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND is_nullable='NO' ORDER BY ordinal_position", (schema, table))
            keys[(schema, table)] = columns[0][0]
            for first, last in ((1, 2), (3, 4)):
                data = io.StringIO()
                writer = csv.writer(data)
                for value in range(first, last + 1):
                    writer.writerow([value if kind == 'bigint' else '2024-01-01' if kind == 'date' else 'done' for _, kind in columns])
                data.seek(0)
                with self.pg.cursor() as cur:
                    if first == 1:
                        for row in csv.reader(data):
                            cur.execute(sql.SQL('INSERT INTO {}.{} ({}) VALUES ({})').format(
                                sql.Identifier(schema), sql.Identifier(table),
                                sql.SQL(',').join(sql.Identifier(c) for c, _ in columns),
                                sql.SQL(',').join(sql.Placeholder() for _ in columns)), row)
                    else:
                        query = sql.SQL('COPY {}.{} ({}) FROM STDIN WITH CSV').format(
                            sql.Identifier(schema), sql.Identifier(table),
                            sql.SQL(',').join(sql.Identifier(c) for c, _ in columns))
                        cur.copy_expert(query.as_string(self.pg), data)
            key = keys[(schema, table)]
            with self.pg.cursor() as cur:
                cur.execute(sql.SQL('UPDATE {}.{} SET {}={} WHERE {}=1').format(
                    sql.Identifier(schema), sql.Identifier(table), sql.Identifier(key), sql.Identifier(key), sql.Identifier(key)))
                cur.execute(sql.SQL('SELECT to_jsonb(t) FROM {}.{} t WHERE {}=1').format(sql.Identifier(schema), sql.Identifier(table), sql.Identifier(key)))
                expected = cur.fetchone()[0]
                cur.execute("SELECT old_data,new_data FROM task3.logs WHERE table_schema=%s AND table_name=%s AND operation='UPDATE'", (schema, table))
                self.assertEqual(cur.fetchone(), (expected, expected))
        for schema, table in reversed(targets):
            with self.pg.cursor() as cur:
                cur.execute(sql.SQL('DELETE FROM {}.{} WHERE {}=1').format(sql.Identifier(schema), sql.Identifier(table), sql.Identifier(keys[(schema, table)])))
        with self.pg.cursor() as cur:
            cur.execute(sql.SQL('TRUNCATE {}').format(sql.SQL(',').join(sql.SQL('{}.{}').format(sql.Identifier(a), sql.Identifier(b)) for a, b in targets)))
        for schema, table in targets:
            self.assertEqual(self.rows('SELECT operation,count(*) FROM task3.logs WHERE table_schema=%s AND table_name=%s GROUP BY operation ORDER BY operation', (schema, table)),
                             [('DELETE', 1), ('INSERT', 4), ('TRUNCATE', 1), ('UPDATE', 1)])
        self.assertEqual(self.rows('SELECT count(*) FROM task3.logs')[0][0], 42)

    def test_populated_task1_regression(self):
        spec = importlib.util.spec_from_file_location('task1_generator', ROOT / 'data/generate_data.py')
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        generated = {}
        def in_memory(path, columns, rows):
            if path.parent == generator.CRAFT_MARKET_DIR:
                generated[path.stem] = (columns, rows)
        with patch.object(generator, 'write_csv', side_effect=in_memory):
            generator.main()
        for table, (columns, rows) in generated.items():
            data = io.StringIO()
            csv.writer(data).writerows(rows)
            data.seek(0)
            with self.pg.cursor() as cur:
                cur.copy_expert(sql.SQL('COPY source3.{} ({}) FROM STDIN WITH CSV').format(
                    sql.Identifier(table), sql.SQL(',').join(sql.Identifier(c) for c in columns)).as_string(self.pg), data)
        query = (ROOT / 'sql/02_query.sql').read_text().replace(':report_year', '2024').replace(':report_month', '1').strip().rstrip(';')
        # Execute the unchanged task1 SQL and rerun audit setup between identical queries.
        with self.pg.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            first = cur.fetchall()
        with self.pg.cursor() as cur:
            cur.execute((ROOT / 'sql/05_task3.sql').read_text())
        with self.pg.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            second = cur.fetchall()
        self.assertEqual(first, second)
        self.assertEqual(len(first), 195)
        self.assertEqual(sum(row['customer_spent'] for row in first), Decimal('8802285.00'))
        self.assertEqual(sum(row['platform_money'] for row in first), Decimal('880228.50'))
        self.assertEqual(sum(row['count_order'] for row in first), 704)
        self.assertEqual(self.rows('SELECT count(*) FROM task3.logs')[0][0], 4230)
        # Exercise the actual unchanged DAG callable, including period DELETE/INSERT.
        with self.pg.cursor() as cur:
            cur.execute((ROOT / 'sql/03_ddl_mart.sql').read_text())
        spec = importlib.util.spec_from_file_location('task1_dag', ROOT / 'dags/task1_customer_report_mart.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        from airflow.models.connection import Connection
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        config = parse_dsn(os.environ['TASK3_PG_DSN'])
        conn = Connection(conn_id='dwh_postgres', conn_type='postgres', host=config['host'],
                          port=int(config.get('port', 5432)), login=config['user'],
                          password=config['password'], schema=self.name)
        with patch.dict(os.environ, {'AIRFLOW_CONN_DWH_POSTGRES': conn.get_uri()}):
            module.load_mart(logical_date=datetime(2024, 1, 1))
            first_mart = self.rows('SELECT to_jsonb(m)-\'id\' FROM mart.craft_market_customer_report m ORDER BY customer_id')
            module.load_mart(logical_date=datetime(2024, 1, 1))
            self.assertEqual(first_mart, self.rows('SELECT to_jsonb(m)-\'id\' FROM mart.craft_market_customer_report m ORDER BY customer_id'))
        self.assertEqual(len(first_mart), 195)
        self.assertEqual(self.rows('SELECT count(*) FROM task3.logs')[0][0], 4230)

    def test_task2_generated_csv_replacement_and_failure_rollback(self):
        spec = importlib.util.spec_from_file_location('task2_dag', ROOT / 'dags/task2_external_source_ingest.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        spec = importlib.util.spec_from_file_location('combined_generator', ROOT / 'data/generate_data.py')
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        from airflow.models.connection import Connection
        config = parse_dsn(os.environ['TASK3_PG_DSN'])
        conn = Connection(conn_id='dwh_postgres', conn_type='postgres', host=config['host'],
                          port=int(config.get('port', 5432)), login=config['user'],
                          password=config['password'], schema=self.name)
        converters = {'date_time': datetime.fromisoformat, 'customer_id': int, 'quantity': int,
                      'payment_amount': Decimal, 'date_id': date.fromisoformat, 'geo_id': int,
                      'sales_qty': int, 'sales_amt': Decimal}
        bad_values = {'user_order_log': ('quantity', '0'),
                      'user_activity_log': ('date_time', 'not-a-timestamp'),
                      'customer_research': ('sales_amt', 'not-a-number')}
        tables = {'user_order_log': ['date_time', 'customer_id', 'quantity', 'payment_amount'],
                  'user_activity_log': ['date_time', 'customer_id'],
                  'customer_research': ['date_id', 'geo_id', 'sales_qty', 'sales_amt']}
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'AIRFLOW_CONN_DWH_POSTGRES': conn.get_uri()}):
            generator.ROOT = Path(folder) / 'data'
            generator.CRAFT_MARKET_DIR = generator.ROOT / 'craft_market'
            generator.EXTERNAL_SOURCE_DIR = generator.ROOT / 'external_source'
            generator.main()
            module.DATA_DIR = generator.EXTERNAL_SOURCE_DIR
            create = module.dag.get_task('create_tables')
            create.render_template_fields(context={})
            create.execute(context={})
            for table, columns in tables.items():
                with self.subTest(table=table):
                    path = module.DATA_DIR / (table + '.csv')
                    with path.open(newline='', encoding='utf-8') as stream:
                        records = list(csv.DictReader(stream))
                    self.assertEqual(len(records), 10)
                    select = sql.SQL('SELECT {} FROM external_source.{}').format(
                        sql.SQL(',').join(map(sql.Identifier, columns)), sql.Identifier(table)).as_string(self.pg)
                    expected = sorted(tuple(converters[c](row[c]) for c in columns) for row in records)
                    load = module.dag.get_task('load_' + table)
                    load.execute(context={})
                    self.assertEqual(sorted(self.rows(select)), expected)
                    load.execute(context={})
                    self.assertEqual(sorted(self.rows(select)), expected)
                    # Reordered and extra source fields must not change the selected projection.
                    changed_key = 'customer_id' if 'customer_id' in columns else 'geo_id'
                    records[0][changed_key] = str(int(records[0][changed_key]) + 1000)
                    for money in ('payment_amount', 'sales_amt'):
                        if money in columns:
                            records[0][money] = '123.45'
                    for row in records:
                        row['ignored'] = 'extra, "quoted"\nучебное поле'
                    header = list(reversed(records[0]))
                    generator.write_csv(path, header, [[r[c] for c in header] for r in records])
                    expected = sorted(tuple(converters[c](row[c]) for c in columns) for row in records)
                    load.execute(context={})
                    self.assertEqual(sorted(self.rows(select)), expected)
                    missing = path.with_suffix('.missing')
                    path.rename(missing)
                    try:
                        with self.assertRaises(FileNotFoundError):
                            load.execute(context={})
                    finally:
                        missing.rename(path)
                    self.assertEqual(sorted(self.rows(select)), expected)
                    path.write_text('ignored\nvalue\n', encoding='utf-8')
                    with self.assertRaisesRegex(ValueError, 'no columns'):
                        load.execute(context={})
                    self.assertEqual(sorted(self.rows(select)), expected)
                    invalid = [dict(records[0]), dict(records[1])]
                    field, value = bad_values[table]
                    invalid[1][field] = value
                    generator.write_csv(path, header, [[r[c] for c in header] for r in invalid])
                    with self.assertRaises(psycopg2.Error):
                        load.execute(context={})
                    self.assertEqual(sorted(self.rows(select)), expected)
                    generator.write_csv(path, header, [])
                    load.execute(context={})
                    self.assertEqual(self.rows(select), [])
            self.assertEqual(self.rows('SELECT count(*) FROM task3.logs'), [(0,)])

    @unittest.skipUnless(os.environ.get('TASK3_SLOW_TESTS'), 'real image/oplog expiration is an explicit slow gate')
    def test_real_image_and_oplog_expiration(self):
        key = self.db.clients.insert_one({'_id': ObjectId(), 'v': 0}).inserted_id
        self.baseline()
        self.db.clients.update_one({'_id': key}, {'$set': {'v': 1}})
        namespace_uuid = self.db.command('listCollections', filter={'name': 'clients'})['cursor']['firstBatch'][0]['info']['uuid']
        admin = self.client.admin
        try:
            admin.command({'setClusterParameter': {'changeStreamOptions': {'preAndPostImages': {'expireAfterSeconds': 1}}}})
            deadline = time.monotonic() + 30
            while self.client.config.system.preimages.count_documents({'_id.nsUUID': namespace_uuid}):
                self.assertLess(time.monotonic(), deadline, 'pre-image expiry timeout')
                time.sleep(.3)
            token = bytes(cdc.state_row(self.pg)['resume_token'])
            try:
                with cdc.watch(self.raw, token) as stream:
                    stream.try_next()
            except OperationFailure as exc:
                print('real required-image loss code:', exc.code, flush=True)
            else:
                self.fail('Required image loss was not raised')
            self.assertEqual(self.run_capture()['status'], 'needs_manual_rebaseline')
            self.assertEqual(bytes(cdc.state_row(self.pg)['resume_token']), token)
        finally:
            admin.command({'setClusterParameter': {'changeStreamOptions': {'preAndPostImages': {'expireAfterSeconds': 'off'}}}})
        self.baseline()
        self.db.clients.insert_one({'_id': ObjectId(), 'after_image_recovery': True})
        self.assertEqual(self.run_capture()['events_committed'], 1)
        token = bytes(cdc.state_row(self.pg)['resume_token'])
        boundary = admin.command('hello')['lastWrite']['opTime']['ts']
        admin.command({'replSetResizeOplog': 1, 'minRetentionHours': 0})
        previous_window = admin.command({'getParameter': 1, 'minSnapshotHistoryWindowInSeconds': 1})['minSnapshotHistoryWindowInSeconds']
        self.addCleanup(lambda: admin.command({'setParameter': 1, 'minSnapshotHistoryWindowInSeconds': previous_window}))
        admin.command({'setParameter': 1, 'minSnapshotHistoryWindowInSeconds': 0})
        filler = self.client['filler_' + uuid.uuid4().hex].events
        payload = 'x' * (1024 * 1024)
        for batch in range(8):
            filler.insert_many([{'_id': ObjectId(), 'payload': payload} for _ in range(32)])
            admin.command({'fsync': 1})
            oldest = self.client.local['oplog.rs'].find_one(sort=[('$natural', 1)])['ts']
            print('oplog filler MiB:', (batch + 1) * 32, flush=True)
            if oldest > boundary:
                break
        deadline = time.monotonic() + 370
        progress = time.monotonic()
        while self.client.local['oplog.rs'].find_one(sort=[('$natural', 1)])['ts'] <= boundary:
            self.assertLess(time.monotonic(), deadline, 'oplog rollover timeout')
            if time.monotonic() - progress >= 30:
                print('waiting for real oplog truncation', flush=True)
                admin.command({'fsync': 1})
                progress = time.monotonic()
            time.sleep(1)
        self.assertEqual(self.run_capture()['status'], 'needs_manual_rebaseline')
        self.assertEqual(bytes(cdc.state_row(self.pg)['resume_token']), token)
        self.assertEqual(self.run_capture()['status'], 'needs_manual_rebaseline')
        self.baseline()
        self.assertEqual(self.rows("SELECT count(*) FROM task3.mongo_events WHERE kind='gap'")[0][0], 2)
        self.db.clients.insert_one({'_id': ObjectId(), 'after_oplog_recovery': True})
        self.assertEqual(self.run_capture()['events_committed'], 1)

    def test_setup_rerun_schema_guard_and_audit(self):
        with self.pg.cursor() as cur:
            cur.execute((ROOT / 'sql/05_task3.sql').read_text())
        self.assertEqual(self.rows("SELECT count(*) FROM pg_trigger WHERE tgname IN ('task3_row_audit','task3_truncate_audit')")[0][0], 12)
        for alteration, restoration in (
            ("ALTER TABLE task3.logs ALTER COLUMN changed_at SET DEFAULT '2000-01-01'::timestamptz",
             "ALTER TABLE task3.logs ALTER COLUMN changed_at SET DEFAULT clock_timestamp()"),
            ("ALTER TABLE task3.logs ALTER COLUMN id SET GENERATED BY DEFAULT",
             "ALTER TABLE task3.logs ALTER COLUMN id SET GENERATED ALWAYS"),
        ):
            with self.pg.cursor() as cur:
                cur.execute(alteration)
            with self.assertRaises(psycopg2.Error):
                with self.pg.cursor() as cur:
                    cur.execute((ROOT / 'sql/05_task3.sql').read_text())
            with self.pg.cursor() as cur:
                cur.execute('ROLLBACK')
                cur.execute(restoration)
                cur.execute((ROOT / 'sql/05_task3.sql').read_text())
        self.rows("INSERT INTO source3.craft_market_customers(customer_id,customer_name) VALUES (1,'one'),(2,'two') RETURNING customer_id")
        self.rows('UPDATE source3.craft_market_customers SET customer_name=customer_name WHERE customer_id=1 RETURNING customer_id')
        self.rows('DELETE FROM source3.craft_market_customers WHERE customer_id=2 RETURNING customer_id')
        self.pg.autocommit = False
        with self.pg.cursor() as cur:
            cur.execute("INSERT INTO source3.craft_market_customers(customer_id) VALUES (3)")
        self.pg.rollback()
        self.pg.autocommit = True
        with self.pg.cursor() as cur:
            cur.execute('TRUNCATE source3.craft_market_orders,source3.craft_market_customers')
        self.assertEqual(self.rows('SELECT operation,count(*) FROM task3.logs GROUP BY operation ORDER BY operation'), [('DELETE', 1), ('INSERT', 2), ('TRUNCATE', 2), ('UPDATE', 1)])
        with self.pg.cursor() as cur:
            cur.execute('ALTER TABLE task3.logs ADD CONSTRAINT injected_failure CHECK (false) NOT VALID')
        with self.assertRaises(psycopg2.IntegrityError):
            self.rows('INSERT INTO source3.craft_market_customers(customer_id) VALUES (4) RETURNING customer_id')
        self.assertEqual(self.rows('SELECT count(*) FROM source3.craft_market_customers')[0][0], 0)
        with self.assertRaises(psycopg2.Error):
            with self.pg.cursor() as cur:
                cur.execute((ROOT / 'sql/05_task3.sql').read_text())
        self.pg.rollback()


if __name__ == '__main__':
    unittest.main()
