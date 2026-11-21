"""G4 preflight. Run only against the dedicated tests/compose.yml project."""
import os
import threading
import time
import uuid

import psycopg2
from bson import ObjectId
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient
from pymongo.read_concern import ReadConcern

uri = os.environ['TASK3_MONGO_URI']
client = MongoClient(uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=5000)
db = client['task3_probe_' + uuid.uuid4().hex]
for name in ('clients', 'orders', 'restaurant'):
    if name not in db.list_collection_names():
        db.create_collection(name, changeStreamPreAndPostImages={'enabled': True})
    db[name].insert_many([{'_id': ObjectId(), 'n': i} for i in range(3)])
raw = db.with_options(codec_options=CodecOptions(document_class=RawBSONDocument), read_concern=ReadConcern('majority'))
pg = psycopg2.connect(os.environ['TASK3_PG_DSN'])
pg.autocommit = True
with pg.cursor() as cur:
    cur.execute('CREATE TABLE IF NOT EXISTS public.handoff_probe (id text PRIMARY KEY, doc bytea NOT NULL)')
assert not client.admin.command('currentOp').get('fsyncLock', False)
receipt = client.admin.command({'fsync': 1, 'lock': True, 'fsyncLockAcquisitionTimeoutMillis': 5000})
assert receipt['lockCount'] == 1
print('fsync lockCount=1', flush=True)
writer_done = threading.Event()
writer_errors = []
writer_id = ObjectId()
def writer():
    try:
        with MongoClient(uri, socketTimeoutMS=30000) as other:
            other[db.name].clients.insert_one({'_id': writer_id, 'after_unlock': True})
        writer_done.set()
    except Exception as exc:
        writer_errors.append(type(exc).__name__)
thread = threading.Thread(target=writer)
try:
    start = client.admin.command('hello')
    target = start['lastWrite']['opTime']['ts']
    deadline = time.monotonic() + 5
    while client.admin.command('hello')['lastWrite']['majorityOpTime']['ts'] < target:
        assert time.monotonic() < deadline, 'majority timeout'
    with raw.watch(full_document='required', full_document_before_change='required', batch_size=1, max_await_time_ms=100) as stream:
        assert stream.try_next() is None
        anchor = stream.resume_token
        assert isinstance(anchor, RawBSONDocument), type(anchor)
        thread.start()
        time.sleep(.3)
        assert not writer_done.is_set(), 'writer escaped fsync'
        pg.autocommit = False
        count = 0
        for name in ('clients', 'orders', 'restaurant'):
            for doc in raw[name].find().batch_size(1):
                assert isinstance(doc, RawBSONDocument)
                with pg.cursor() as cur:
                    cur.execute('INSERT INTO public.handoff_probe VALUES (%s,%s)', (str(doc['_id']), doc.raw))
                    cur.execute('SELECT doc FROM public.handoff_probe WHERE id=%s', (str(doc['_id']),))
                    assert bytes(cur.fetchone()[0]) == doc.raw
                count += 1
        assert stream.try_next() is None
        assert client.admin.command('currentOp').get('fsyncLock', False)
        assert client.admin.command('hello')['topologyVersion']['processId'] == start['topologyVersion']['processId']
        assert not writer_done.is_set()
        pg.commit()
        print(f'atomic baseline={count}; raw find/getMore and empty public token OK', flush=True)
    unlocked = client.admin.command('fsyncUnlock')
    assert unlocked['lockCount'] == 0
    thread.join(10)
    assert writer_done.is_set() and not writer_errors, writer_errors
    with raw.watch(resume_after=anchor, full_document='required', full_document_before_change='required', batch_size=1, max_await_time_ms=100) as stream:
        event = None
        for _ in range(20):
            event = stream.try_next()
            if event is not None:
                break
        assert isinstance(event, RawBSONDocument)
        assert isinstance(event['_id'], RawBSONDocument)
        assert isinstance(event['fullDocument'], RawBSONDocument)
        assert event['documentKey']['_id'] == writer_id
        print('first blocked/post-unlock write resumed from raw anchor exactly once', flush=True)
        assert stream.try_next() is None
finally:
    pg.rollback()
    pg.close()
    # This probe owns exactly the lock it acquired; never consume another lock.
    if client.admin.command('currentOp').get('fsyncLock', False):
        assert client.admin.command('fsyncUnlock')['lockCount'] == 0
    thread.join(10) if thread.ident else None
    client.close()
