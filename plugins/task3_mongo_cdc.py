"""Task 3: bounded raw-BSON capture for a supervised, local teaching replica set."""
import argparse
import json
import os
import time
from contextlib import closing, contextmanager

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from bson import ObjectId, json_util
from bson.codec_options import CodecOptions, DatetimeConversion
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient, ReadPreference
from pymongo.errors import OperationFailure, PyMongoError
from pymongo.read_concern import ReadConcern

COLLECTIONS = ('clients', 'orders', 'restaurant')
LOCK_KEY = 3141592653589793
READER_VERSION = 'raw-required-v1'
PROJECTION_VERSION = 'relaxed-ejson-v1'
RAW_CODEC = CodecOptions(document_class=RawBSONDocument, unicode_decode_error_handler='strict',
                         datetime_conversion=DatetimeConversion.DATETIME_AUTO)
MAX_BUFFER_BYTES = 4 * 1024 * 1024
BASELINE_MAX_DOCUMENTS = 10000
BASELINE_MAX_BYTES = 50 * 1024 * 1024
BATCH_EVENTS = 50
RUN_EVENTS = 500
READ_SECONDS = 20
BASELINE_SECONDS = 90
# MongoDB's explicit history/image-loss errors; unrelated operation errors never reset state.
LOSS_CODES = {136, 286}


class ContractError(RuntimeError):
    pass


class ContinuityError(ContractError):
    pass


class PublicationUnknown(ContractError):
    pass


def mongo_client(uri):
    return MongoClient(uri, appname='task3-cdc', serverSelectionTimeoutMS=5000,
                       connectTimeoutMS=5000, socketTimeoutMS=8000, waitQueueTimeoutMS=5000)


def raw_bytes(document):
    if not isinstance(document, RawBSONDocument):
        raise ContractError('Expected public RawBSONDocument')
    return document.raw


def document_id(document):
    value = document.get('_id')
    if not isinstance(value, ObjectId):
        raise ContractError('Only ObjectId document identifiers are supported')
    return str(value)


def identity(db):
    config = db.client.admin.command('replSetGetConfig')['config']
    hello = db.client.admin.command('hello')
    if not hello.get('isWritablePrimary') or len(config['members']) != 1:
        raise ContractError('Expected one primary replica-set member')
    info = {row['name']: row for row in db.list_collections()}
    uuids = {}
    for name in COLLECTIONS:
        row = info.get(name)
        if row is None or row.get('type') != 'collection':
            raise ContractError('Missing watched collection: ' + name)
        if not row.get('options', {}).get('changeStreamPreAndPostImages', {}).get('enabled'):
            raise ContractError('Required pre/post images disabled: ' + name)
        uuids[name] = bytes(row['info']['uuid']).hex()
    return {'replica_set_id': str(config['settings']['replicaSetId']),
            'database': db.name, 'collections': uuids}


def validate_binding(state, actual):
    if state['reader_version'] != READER_VERSION or state['projection_version'] != PROJECTION_VERSION:
        raise ContractError('Reader/projection version mismatch; explicit migration required')
    if state['identity'] is not None and state['identity'] != actual:
        raise ContractError('Immutable source binding mismatch')


def paused_process(db):
    admin = db.client.admin
    hello = admin.command('hello')
    if not hello.get('isWritablePrimary') or not admin.command('currentOp').get('fsyncLock', False):
        raise ContractError('Operator-held fsync lock required')
    return (str(hello['topologyVersion']['processId']), hello.get('electionId'), hello.get('me'))


def majority_barrier(db, process):
    target = db.client.admin.command('hello')['lastWrite']['opTime']
    deadline = time.monotonic() + 10
    while True:
        if paused_process(db) != process:
            raise ContractError('Source process/primary changed during pause')
        majority = db.client.admin.command('hello')['lastWrite']['majorityOpTime']
        if (majority['t'], majority['ts']) >= (target['t'], target['ts']):
            return
        if time.monotonic() >= deadline:
            raise ContractError('Majority barrier timed out')
        time.sleep(.05)


def watch(db, token=None):
    pipeline = [{'$match': {'$or': [
        {'ns.coll': {'$in': list(COLLECTIONS)}},
        {'to.db': db.name, 'to.coll': {'$in': list(COLLECTIONS)}},
        {'operationType': {'$in': ['dropDatabase', 'invalidate']}}
    ]}}]
    options = dict(full_document='required', full_document_before_change='required',
                   max_await_time_ms=500, batch_size=BATCH_EVENTS)
    if token is not None:
        options['resume_after'] = RawBSONDocument(bytes(token), RAW_CODEC)
    return db.watch(pipeline, **options)


def state_row(pg):
    with pg.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT * FROM task3.mongo_state WHERE singleton')
        state = cur.fetchone()
    if state is None:
        raise ContractError('Missing singleton; run SQL setup')
    return state


@contextmanager
def store_lock(pg):
    pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute("SET statement_timeout='10s'")
        cur.execute("SET lock_timeout='5s'")
        cur.execute("SET idle_in_transaction_session_timeout='120s'")
        cur.execute('SELECT pg_try_advisory_lock(%s)', (LOCK_KEY,))
        owned = cur.fetchone()[0]
    try:
        yield owned
    finally:
        if not pg.closed:
            pg.rollback()
            pg.autocommit = True
            if owned:
                with pg.cursor() as cur:
                    cur.execute('SELECT pg_advisory_unlock(%s)', (LOCK_KEY,))


def projection(cur, document):
    try:
        text = json_util.dumps(document, json_options=json_util.RELAXED_JSON_OPTIONS,
                               ensure_ascii=True, allow_nan=False)
    except (UnicodeError, ValueError, OverflowError) as exc:
        return None, 'unavailable', type(exc).__name__
    cur.execute('SAVEPOINT projection')
    try:
        cur.execute('SELECT %s::jsonb', (text,))
    except psycopg2.Error as exc:
        cur.execute('ROLLBACK TO SAVEPOINT projection')
        if exc.pgcode not in {'22P02', '22P05', '22003', '54001'}:
            raise
        return None, 'unavailable', 'jsonb_sqlstate_' + exc.pgcode
    finally:
        cur.execute('RELEASE SAVEPOINT projection')
    return text, 'available', None


def latest(cur, collection, key, epoch, document, projected):
    cur.execute('''INSERT INTO task3.mongo_latest
        (collection,document_id,epoch,raw_bson,document,projection_status,projection_reason)
        VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s)
        ON CONFLICT (collection,document_id) DO UPDATE SET epoch=EXCLUDED.epoch,
        raw_bson=EXCLUDED.raw_bson,document=EXCLUDED.document,
        projection_status=EXCLUDED.projection_status,projection_reason=EXCLUDED.projection_reason''',
                (collection, key, epoch, raw_bytes(document), *projected))


def prepare_event(event):
    raw = raw_bytes(event)
    operation = event['operationType']
    collection = event.get('ns', {}).get('coll')
    if collection not in COLLECTIONS or operation not in {'insert', 'update', 'replace', 'delete'}:
        raise ContractError('Unsupported watched lifecycle/event: ' + operation)
    key = document_id(event['documentKey'])
    if operation in {'update', 'replace', 'delete'} and not isinstance(event.get('fullDocumentBeforeChange'), RawBSONDocument):
        raise ContinuityError('Required before-image missing')
    selected = event.get('fullDocumentBeforeChange') if operation == 'delete' else event.get('fullDocument')
    if not isinstance(selected, RawBSONDocument):
        raise ContinuityError('Required selected image missing')
    if document_id(selected) != key:
        raise ContractError('Image/document key mismatch')
    token = raw_bytes(event['_id'])
    if len(raw) + len(token) + len(raw_bytes(selected)) > MAX_BUFFER_BYTES:
        raise ContractError('Event exceeds fixture buffer limit')
    return collection, key, operation, selected, raw, token, event['clusterTime']


def apply_batch(pg, db, state, events, token, empty=False):
    pg.autocommit = False
    unavailable = 0
    inserted = 0
    try:
        with pg.cursor() as cur:
            for collection, key, operation, selected, raw, event_token, ts in events:
                projected = projection(cur, selected)
                if len(raw) + len(event_token) + len(raw_bytes(selected)) + len((projected[0] or '').encode()) > MAX_BUFFER_BYTES:
                    raise ContractError('Projected event exceeds fixture buffer limit')
                cur.execute('''INSERT INTO task3.mongo_events
                    (epoch,kind,collection,document_id,operation,token,cluster_seconds,cluster_increment,
                     raw_bson,document,projection_status,projection_reason)
                    VALUES (%s,'change',%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)
                    ON CONFLICT (token) DO NOTHING RETURNING id''',
                            (state['epoch'], collection, key, operation, event_token, ts.time, ts.inc, raw, *projected))
                if cur.fetchone() is None:
                    continue  # A replay must never regress current state to its older image.
                inserted += 1
                unavailable += projected[1] == 'unavailable'
                if operation == 'delete':
                    cur.execute('DELETE FROM task3.mongo_latest WHERE collection=%s AND document_id=%s', (collection, key))
                else:
                    latest(cur, collection, key, state['epoch'], selected, projected)
            validate_binding(state, identity(db))
            cur.execute('''UPDATE task3.mongo_state SET resume_token=%s,last_commit_at=clock_timestamp(),
                last_empty_at=CASE WHEN %s THEN clock_timestamp() ELSE last_empty_at END,failure=NULL
                WHERE singleton''', (token, empty))
        pg.commit()
    except BaseException:
        pg.rollback()
        raise
    finally:
        if not pg.closed:
            pg.autocommit = True
    return inserted, unavailable


def baseline(pg, db, state, pause_held, pg_factory):
    if state['status'] == 'active':
        raise ContractError('Baseline rejects active; do not rewind the checkpoint')
    if not pause_held:
        raise ContractError('Explicit assertion of continuously held operator pause is required')
    started = time.monotonic()
    actual = identity(db)
    validate_binding(state, actual)
    process = paused_process(db)
    majority_barrier(db, process)
    epoch = state['epoch'] + 1
    count = size = unavailable = 0
    with watch(db) as stream:
        if stream.try_next() is not None:
            raise ContractError('Unexpected mutation under source pause')
        anchor = raw_bytes(stream.resume_token)
        pg.autocommit = False
        try:
            with pg.cursor() as cur:
                cur.execute('DELETE FROM task3.mongo_latest')
                cur.execute("INSERT INTO task3.mongo_events(epoch,kind,projection_status,metadata) VALUES (%s,'epoch','metadata',%s)",
                            (epoch, Json({'anchor_hex': anchor.hex()})))
                for collection in COLLECTIONS:
                    with db[collection].find().batch_size(50).max_time_ms(10000) as cursor:
                        for doc in cursor:
                            if time.monotonic() - started >= BASELINE_SECONDS:
                                raise ContractError('Baseline time limit exceeded')
                            raw = raw_bytes(doc)
                            key = document_id(doc)
                            if len(raw) > MAX_BUFFER_BYTES:
                                raise ContractError('Document exceeds fixture buffer limit')
                            projected = projection(cur, doc)
                            count += 1
                            size += 2 * len(raw) + 2 * len((projected[0] or '').encode())
                            if count > BASELINE_MAX_DOCUMENTS or size > BASELINE_MAX_BYTES:
                                raise ContractError('Cumulative baseline limit exceeded')
                            unavailable += projected[1] == 'unavailable'
                            cur.execute('''INSERT INTO task3.mongo_events
                                (epoch,kind,collection,document_id,raw_bson,document,projection_status,projection_reason)
                                VALUES (%s,'baseline',%s,%s,%s,%s::jsonb,%s,%s)''',
                                        (epoch, collection, key, raw, *projected))
                            latest(cur, collection, key, epoch, doc, projected)
                if stream.try_next() is not None or not stream.alive:
                    raise ContractError('Stream changed or closed under pause')
                raw_bytes(stream.resume_token)
                if paused_process(db) != process or identity(db) != actual:
                    raise ContractError('Source identity/options/process/lock changed during baseline')
                if time.monotonic() - started >= BASELINE_SECONDS:
                    raise ContractError('Baseline time limit exceeded')
                if state['status'] == 'needs_manual_rebaseline':
                    cur.execute("INSERT INTO task3.mongo_events(epoch,kind,projection_status,metadata) VALUES (%s,'gap','metadata',%s)",
                                (epoch, Json({'previous_epoch': state['epoch'],
                                              'previous_token_hex': bytes(state['resume_token']).hex(),
                                              'reason': state['failure'], 'new_anchor_hex': anchor.hex()})))
                cur.execute('''UPDATE task3.mongo_state SET status='active',identity=%s,epoch=%s,
                    resume_token=%s,last_commit_at=clock_timestamp(),failure=NULL WHERE singleton''',
                            (Json(actual), epoch, anchor))
            try:
                pg.commit()
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                # Lost acknowledgement is not permission to publish a second epoch.
                pg.close()
                if pg_factory is None:
                    raise PublicationUnknown('Commit outcome unknown; inspect durable state') from exc
                try:
                    with closing(pg_factory()) as check:
                        saved = state_row(check)
                    if saved['epoch'] != epoch or bytes(saved['resume_token'] or b'') != anchor or saved['identity'] != actual:
                        raise PublicationUnknown('Expected epoch/token not confirmed; inspect before retry') from exc
                except psycopg2.Error as read_error:
                    raise PublicationUnknown('Commit readback unavailable; inspect before retry') from read_error
        except BaseException:
            if not pg.closed:
                pg.rollback()
            raise
        finally:
            if not pg.closed:
                pg.autocommit = True
    return {'status': 'baseline_published', 'epoch': epoch, 'documents': count,
            'bytes': size, 'unavailable_projections': unavailable, 'checkpoint_hex': anchor.hex()}


def source_loss(exc):
    if isinstance(exc, ContinuityError) or exc.code in LOSS_CODES:
        return True
    message = (exc.details or {}).get('errmsg', str(exc)).lower()
    if exc.code == 47:
        return ('pre-image' in message or 'post-image' in message) and 'not found' in message
    return exc.code == 280 and 'resume token' in message and ('not found' in message or 'cannot resume' in message)


def consume(pg, db, state):
    if state['status'] != 'active':
        return {'status': 'manual_baseline_required' if state['status'] == 'uninitialized' else 'needs_manual_rebaseline',
                'epoch': state['epoch']}
    validate_binding(state, identity(db))
    deadline = time.monotonic() + READ_SECONDS
    read = inserted = unavailable = buffered = 0
    batch = []
    token = bytes(state['resume_token'])
    outcome = 'budget_reached'
    try:
        with watch(db, token) as stream:
            while read < RUN_EVENTS and time.monotonic() < deadline:
                event = stream.try_next()
                next_token = raw_bytes(stream.resume_token)
                if event is None:
                    n, errors = apply_batch(pg, db, state, batch, next_token, empty=True)
                    inserted += n
                    unavailable += errors
                    batch = []
                    outcome = 'empty_at_read'
                    token = next_token
                    break
                prepared = prepare_event(event)
                event_size = len(prepared[4]) + len(prepared[5]) + len(raw_bytes(prepared[3]))
                if batch and buffered + event_size > MAX_BUFFER_BYTES:
                    n, errors = apply_batch(pg, db, state, batch, token)
                    inserted += n
                    unavailable += errors
                    batch = []
                    buffered = 0
                batch.append(prepared)
                buffered += event_size
                read += 1
                token = next_token
                if len(batch) >= BATCH_EVENTS:
                    n, errors = apply_batch(pg, db, state, batch, token)
                    inserted += n
                    unavailable += errors
                    batch = []
                    buffered = 0
            if batch:
                n, errors = apply_batch(pg, db, state, batch, token)
                inserted += n
                unavailable += errors
    except (OperationFailure, ContinuityError) as exc:
        if not source_loss(exc):
            raise
        pg.rollback()
        pg.autocommit = True
        with pg.cursor() as cur:
            cur.execute("UPDATE task3.mongo_state SET status='needs_manual_rebaseline',failure=%s WHERE singleton",
                        (Json({'type': type(exc).__name__, 'code': getattr(exc, 'code', None)}),))
        return {'status': 'needs_manual_rebaseline', 'epoch': state['epoch'],
                'events_committed': inserted, 'unavailable_projections': unavailable}
    return {'status': outcome, 'epoch': state['epoch'], 'events_read': read,
            'events_committed': inserted, 'unavailable_projections': unavailable, 'checkpoint_hex': token.hex()}


def run(pg, client, database, command='consume', *, pause_held=False, pg_factory=None):
    started = time.monotonic()
    db = client.get_database(database, codec_options=RAW_CODEC, read_preference=ReadPreference.PRIMARY,
                             read_concern=ReadConcern('majority'))
    with store_lock(pg) as owned:
        if not owned:
            return {'status': 'contended'}
        state = state_row(pg)
        if command == 'baseline':
            result = baseline(pg, db, state, pause_held, pg_factory)
        elif command == 'consume':
            result = consume(pg, db, state)
        else:
            raise ContractError('Unknown command')
        result['elapsed_seconds'] = round(time.monotonic() - started, 3)
        return result


def airflow_run(command='consume', pause_held=False):
    from airflow.hooks.base import BaseHook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    mongo = BaseHook.get_connection('task3_mongo')
    factory = PostgresHook(postgres_conn_id='dwh_postgres').get_conn
    pg = factory()
    try:
        with mongo_client(mongo.get_uri()) as client:
            result = run(pg, client, mongo.schema, command, pause_held=pause_held, pg_factory=factory)
        print(json.dumps(result, sort_keys=True))
        return result
    except (PyMongoError, psycopg2.Error) as exc:
        raise ContractError('Capture failed: ' + type(exc).__name__ + '; code=' + str(getattr(exc, 'code', None) or getattr(exc, 'pgcode', None))) from None
    finally:
        pg.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['baseline', 'consume'])
    parser.add_argument('--pause-held', action='store_true', help='Operator asserts one continuously held fsync lock since acquisition')
    args = parser.parse_args()
    if 'TASK3_PG_DSN' not in os.environ:
        airflow_run(args.command, args.pause_held)
        return
    factory = lambda: psycopg2.connect(os.environ['TASK3_PG_DSN'], connect_timeout=5)
    pg = factory()
    try:
        with mongo_client(os.environ['TASK3_MONGO_URI']) as client:
            result = run(pg, client, os.environ.get('TASK3_MONGO_DATABASE', 'task3'), args.command,
                         pause_held=args.pause_held, pg_factory=factory)
        print(json.dumps(result, sort_keys=True))
    except (PyMongoError, psycopg2.Error) as exc:
        raise ContractError('Capture failed: ' + type(exc).__name__ + '; code=' + str(getattr(exc, 'code', None) or getattr(exc, 'pgcode', None))) from None
    finally:
        pg.close()


if __name__ == '__main__':
    main()
