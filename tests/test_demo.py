"""Ownership and fail-closed checks for the portable demo; no external services."""
from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, Mock, call, patch

SPEC = importlib.util.spec_from_file_location('demo', Path(__file__).resolve().parents[1] / 'scripts/demo.py')
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class DemoTests(unittest.TestCase):
    def admin(self):
        admin = Mock()
        admin.command.side_effect = [
            {'fsyncLock': False}, {'lockCount': 1}, {'lockCount': 0},
        ]
        return admin

    def test_baseline_unlocks_only_after_confirmed_publication(self):
        admin = self.admin()
        publish = Mock(return_value={'status': 'baseline_published', 'epoch': 1})
        self.assertEqual(demo.publish_baseline(admin, publish)['epoch'], 1)
        self.assertEqual(admin.command.call_args_list, [
            call('currentOp'), call({'fsync': 1, 'lock': True}), call('fsyncUnlock'),
        ])
        publish.assert_called_once_with()

    def test_existing_lock_is_never_acquired_or_released(self):
        admin = self.admin()
        admin.command.side_effect = None
        admin.command.return_value = {'fsyncLock': True}
        publish = Mock()
        with self.assertRaisesRegex(RuntimeError, 'уже заблокирована'):
            demo.publish_baseline(admin, publish)
        admin.command.assert_called_once_with('currentOp')
        publish.assert_not_called()

    def test_failed_or_ambiguous_publication_leaves_lock(self):
        for result in ('failure', 'contended', 'needs_manual_rebaseline'):
            with self.subTest(result=result):
                admin = self.admin()
                publish = Mock(side_effect=RuntimeError('unknown commit')) if result == 'failure' else Mock(
                    return_value={'status': result})
                with redirect_stderr(io.StringIO()) as output, self.assertRaises(RuntimeError):
                    demo.publish_baseline(admin, publish)
                self.assertNotIn(call('fsyncUnlock'), admin.command.call_args_list)
                self.assertIn('Блокировка MongoDB сохранена', output.getvalue())

    def test_multiple_lock_count_never_publishes_or_unlocks(self):
        admin = self.admin()
        admin.command.side_effect = [{'fsyncLock': False}, {'lockCount': 2}]
        publish = Mock()
        with redirect_stderr(io.StringIO()), self.assertRaises(RuntimeError):
            demo.publish_baseline(admin, publish)
        publish.assert_not_called()
        self.assertNotIn(call('fsyncUnlock'), admin.command.call_args_list)

    def test_manual_recovery_rejected_before_binding_or_source_changes(self):
        db, cdc = Mock(), Mock()
        db.client.admin.command.return_value = {'fsyncLock': False}
        with self.assertRaisesRegex(RuntimeError, 'needs_manual_rebaseline'):
            demo.check_source({'status': 'needs_manual_rebaseline'}, db, cdc)
        cdc.identity.assert_not_called()
        cdc.validate_binding.assert_not_called()

    def test_active_source_binding_is_checked(self):
        db, cdc = Mock(), Mock()
        db.client.admin.command.return_value = {'fsyncLock': False}
        state = {'status': 'active'}
        demo.check_source(state, db, cdc)
        cdc.identity.assert_called_once_with(db)
        cdc.validate_binding.assert_called_once_with(state, cdc.identity.return_value)

    def test_fixture_seed_runs_only_before_first_baseline(self):
        root = Path(__file__).resolve().parents[1]
        for status in ('uninitialized', 'active'):
            pg, client, cdc = MagicMock(), MagicMock(), MagicMock()
            pg.closed = False
            pg.cursor.return_value.__enter__.return_value.fetchone.return_value = (1,)
            client.admin.command.return_value = {'fsyncLock': False}
            client.__getitem__.return_value.client = client
            cdc.mongo_client.return_value.__enter__.return_value = client
            cdc.state_row.return_value = {'status': status}
            hook = Mock()
            hook.get_conn.return_value = pg
            modules = {
                'airflow.hooks.base': Mock(BaseHook=Mock()),
                'airflow.providers.postgres.hooks.postgres': Mock(PostgresHook=Mock(return_value=hook)),
                'task3_mongo_cdc': cdc,
            }
            with self.subTest(status=status), patch.dict(sys.modules, modules), patch.object(demo, 'ROOT', root):
                with patch.object(demo.runpy, 'run_path') as seed, patch.object(demo, 'publish_baseline') as publish:
                    with patch.object(demo, 'execute_dag', side_effect=StopIteration), redirect_stdout(io.StringIO()):
                        with self.assertRaises(StopIteration):
                            demo.demonstrate()
                    if status == 'uninitialized':
                        seed.assert_called_once_with(str(root / 'mongo/task3-seed.py'), run_name='__main__')
                        publish.assert_called_once()
                    else:
                        seed.assert_not_called()
                        publish.assert_not_called()

    def test_contended_demo_lock_never_enters_or_unlocks(self):
        pg = MagicMock()
        cur = pg.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (False,)
        with self.assertRaisesRegex(RuntimeError, 'уже запущена'):
            with demo.demo_lock(pg):
                self.fail('contended run entered')
        self.assertEqual(cur.execute.call_args_list, [call('SELECT pg_try_advisory_lock(%s, %s)', demo.DEMO_LOCK)])


if __name__ == '__main__':
    unittest.main()
