"""Run with parent-controlled restart of only the isolated Mongo test container."""
import time
from unittest.mock import patch
from bson import ObjectId
import task3_mongo_cdc as cdc
from test_task3 import CaptureTests

case = CaptureTests()
case.setUp()
try:
    case.db.clients.insert_one({'_id': ObjectId(), 'restart_probe': True})
    before = case.snapshot()
    original = cdc.latest
    def staged(*args):
        original(*args)
        print('RESTART_READY: one row staged under owned fsync lock', flush=True)
        time.sleep(40)
    with patch.object(cdc, 'latest', side_effect=staged):
        try:
            case.baseline()
        except Exception as exc:
            print('baseline rejected restarted source:', type(exc).__name__, flush=True)
        else:
            raise AssertionError('Restarted source was accepted')
    assert case.snapshot() == before
    assert not case.client.admin.command('currentOp').get('fsyncLock', False)
    print('PASS: source restart left old PG publication unchanged and released server lock', flush=True)
finally:
    case.doCleanups()
