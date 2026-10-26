import unittest
from browser_lifecycle import BrowserSessions


class BrowserLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.sessions = BrowserSessions(clock=lambda: self.now)

    def test_reload_reconnect_cancels_shutdown(self):
        instance = self.sessions.heartbeat("old-page")
        self.sessions.close("old-page", instance)
        self.now = 3
        self.assertFalse(self.sessions.should_shutdown())
        self.sessions.heartbeat("new-page")
        self.now = 60
        self.assertFalse(self.sessions.should_shutdown())

    def test_last_page_close_waits_for_grace(self):
        instance = self.sessions.heartbeat("page")
        self.sessions.close("page", instance)
        self.now = 14
        self.assertFalse(self.sessions.should_shutdown())
        self.now = 16
        self.assertTrue(self.sessions.should_shutdown())

    def test_other_page_and_stale_instance_cannot_stop_server(self):
        instance = self.sessions.heartbeat("one")
        self.sessions.heartbeat("two")
        self.sessions.close("one", instance)
        self.sessions.legacy_close()
        self.sessions.close("two", "previous-app-instance")
        self.now = 60
        self.assertFalse(self.sessions.should_shutdown())
        self.sessions.close("two", instance)
        self.now = 80
        self.assertTrue(self.sessions.should_shutdown())

    def test_server_without_browser_does_not_stop_on_startup(self):
        self.now = 1000
        self.assertFalse(self.sessions.should_shutdown())
