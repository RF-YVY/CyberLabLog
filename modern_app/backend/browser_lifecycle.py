"""Track app pages without treating navigation or reload as immediate exit."""
import threading
import time
import uuid


class BrowserSessions:
    def __init__(self, grace_seconds=15, clock=time.monotonic):
        self.instance_id = uuid.uuid4().hex
        self.grace_seconds = grace_seconds
        self.clock = clock
        self.active = set()
        self.close_requested_at = None
        self.lock = threading.Lock()

    def heartbeat(self, session_id):
        with self.lock:
            self.active.add(session_id)
            self.close_requested_at = None
        return self.instance_id

    def close(self, session_id, instance_id):
        with self.lock:
            if instance_id != self.instance_id or session_id not in self.active:
                return
            self.active.remove(session_id)
            if not self.active:
                self.close_requested_at = self.clock()

    def legacy_close(self):
        with self.lock:
            if not self.active:
                self.close_requested_at = self.clock()

    def should_shutdown(self):
        with self.lock:
            return not self.active and self.close_requested_at is not None and self.clock() - self.close_requested_at >= self.grace_seconds
