"""Auth regression tests. Dummy credentials only; no email or live auth calls."""
import unittest
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch

import httpx
from streamlit.testing.v1 import AppTest
from supabase import create_client
from supabase.lib.client_options import SyncClientOptions

import web_ui


class ClientTests(unittest.TestCase):
    def test_real_sync_client_constructs_without_network(self):
        with patch.object(httpx.Client, 'send', side_effect=AssertionError('Network forbidden')):
            client = create_client('https://example.supabase.co', 'dummy-public-key',
                                   options=SyncClientOptions(flow_type='implicit'))
            self.assertIsNotNone(client.auth)
            self.assertIsNotNone(client.storage)

    def test_initialization_error_is_not_missing_configuration(self):
        # Streamlit session state supports both mapping and attribute access.
        class State(dict):
            __getattr__ = dict.get
            __setattr__ = dict.__setitem__
        state = State()
        with patch.object(web_ui.st, 'session_state', state), \
             patch.object(web_ui, 'supabase_settings', return_value=('https://example.supabase.co', 'dummy')), \
             patch.object(web_ui, 'create_client', side_effect=ValueError('secret-do-not-show')):
            with self.assertLogs('web_ui') as logs:
                self.assertIsNone(web_ui.get_supabase_client())
            self.assertEqual(state.web_auth_status, 'initialization')
            self.assertNotIn('secret-do-not-show', str(logs.output))

    def test_callback_never_exchanges_or_accepts_query_tokens(self):
        at = AppTest.from_string('import web_ui; web_ui.recover_password_session(None)')
        at.query_params.update(code='dummy', access_token='dummy', refresh_token='dummy', stock='2330')
        at.run()
        self.assertEqual(at.query_params, {'stock': '2330'})
        self.assertTrue(at.session_state['web_auth_open'])
        self.assertNotIn('web_password_recovery', at.session_state)


class AuthPageTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.auth.sign_in_with_password.return_value = NS(user=NS(id='u'), session=NS())
        self.client.auth.sign_up.return_value = NS(user=NS(id='u'), session=None)
        self.client.auth.verify_otp.return_value = NS(user=NS(id='u'), session=NS())
        self.patch = patch.object(web_ui, 'get_supabase_client', return_value=self.client)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.at = AppTest.from_string('import web_ui; web_ui.render_auth_page()').run()

    def mode(self, mode):
        self.at.radio(key='web_auth_mode').set_value(mode).run()

    def credentials(self, password='strong-password'):
        self.at.text_input(key='web_auth_email').set_value('test@example.com')
        self.at.text_input(key='web_auth_password').set_value(password)

    def test_signup_confirmation_blocks_request_and_status_is_generic(self):
        self.mode('建立帳號')
        self.credentials()
        self.at.button(key='FormSubmitter:supabase_password_auth-建立帳號').click().run()
        self.client.auth.sign_up.assert_not_called()
        self.credentials()
        next(x for x in self.at.text_input if x.label == '確認密碼').set_value('strong-password')
        self.at.button(key='FormSubmitter:supabase_password_auth-建立帳號').click().run()
        self.client.auth.sign_up.assert_called_once()
        self.assertTrue(any('若此信箱符合' in x.value for x in self.at.info))
        self.assertNotIn('web_supabase_user', self.at.session_state)

    def test_login_requires_session_and_sanitizes_errors(self):
        self.client.auth.sign_in_with_password.return_value = NS(user=NS(id='u'), session=None)
        self.credentials()
        self.at.button(key='FormSubmitter:supabase_password_auth-登入').click().run()
        self.assertNotIn('web_supabase_user', self.at.session_state)
        self.client.auth.sign_in_with_password.side_effect = RuntimeError('secret-do-not-show')
        self.credentials()
        self.at.button(key='FormSubmitter:supabase_password_auth-登入').click().run()
        self.assertNotIn('secret-do-not-show', str([x.value for x in self.at.error]))
        self.assertEqual(len(self.at.exception), 0)

    def test_successful_login_and_logout_do_not_require_family_setup(self):
        self.credentials()
        self.at.button(key='FormSubmitter:supabase_password_auth-登入').click().run()
        self.assertEqual(self.at.session_state['web_supabase_user'].id, 'u')
        self.client.table.assert_not_called()
        self.at.button(key='web_supabase_signout').click().run()
        self.assertNotIn('web_supabase_user', self.at.session_state)

    def test_recovery_disabled_does_not_send(self):
        self.mode('忘記密碼')
        self.assertTrue(any('尚待管理員' in x.value for x in self.at.warning))
        self.client.auth.reset_password_for_email.assert_not_called()

    def test_new_session_recovery_otp_then_update(self):
        with patch.object(web_ui, 'recovery_enabled', return_value=True):
            self.mode('忘記密碼')
            next(x for x in self.at.text_input if x.label == '收信 Email').set_value('test@example.com')
            next(x for x in self.at.text_input if x.label == '信中的驗證碼').set_value('dummy-code')
            self.at.button(key='FormSubmitter:supabase_recovery_verify-驗證並設定新密碼').click().run()
            self.client.auth.verify_otp.assert_called_once_with(
                {'email': 'test@example.com', 'token': 'dummy-code', 'type': 'recovery'})
            self.assertNotIn('web_supabase_user', self.at.session_state)
            self.assertTrue(self.at.session_state['web_password_recovery'])
            self.at.text_input[0].set_value('new-long-password')
            self.at.text_input[1].set_value('new-long-password')
            self.at.button(key='FormSubmitter:supabase_password_recovery-更新密碼').click().run()
            self.client.auth.update_user.assert_called_once_with({'password': 'new-long-password'})
            self.client.auth.sign_out.assert_called_once()
            self.assertNotIn('web_password_recovery', self.at.session_state)
            self.assertEqual(len(self.at.exception), 0)


class BootstrapTests(unittest.TestCase):
    def client_for(self, data):
        client = MagicMock()
        for name, rows in data.items():
            query = MagicMock()
            for method in ('select', 'eq', 'limit', 'order', 'insert'):
                getattr(query, method).return_value = query
            query.execute.return_value = NS(data=rows)
            data[name] = query
        client.table.side_effect = lambda name: data[name]
        return client, data

    def test_existing_shared_member_never_creates_family(self):
        client, tables = self.client_for({'family_members': [{'family_id': 'shared', 'role': 'member'}],
                                          'watchlists': [{'id': 'list'}]})
        self.assertEqual(web_ui.bootstrap_shared_watchlist(client, NS(id='u')), ('shared', 'list'))
        tables['family_members'].insert.assert_not_called()
        tables['watchlists'].insert.assert_not_called()

    def test_existing_owner_missing_membership_is_repaired(self):
        client, tables = self.client_for({'family_members': [], 'family_groups': [{'id': 'own'}],
                                          'watchlists': [{'id': 'list'}]})
        self.assertEqual(web_ui.bootstrap_shared_watchlist(client, NS(id='u')), ('own', 'list'))
        tables['family_groups'].insert.assert_not_called()
        tables['family_members'].insert.assert_called_once_with({'family_id': 'own', 'user_id': 'u', 'role': 'owner'})

    def test_member_missing_shared_list_does_not_escalate(self):
        client, tables = self.client_for({'family_members': [{'family_id': 'shared', 'role': 'member'}],
                                          'watchlists': []})
        with self.assertRaises(ValueError):
            web_ui.bootstrap_shared_watchlist(client, NS(id='u'))
        tables['watchlists'].insert.assert_not_called()


if __name__ == '__main__':
    unittest.main()
