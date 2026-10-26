import base64
from contextlib import closing
import hashlib
import io
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from fastapi import HTTPException

import main
import update_installer


class UpdateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.content = b"MZ" + b"test executable" * 100
        self.sha = hashlib.sha256(self.content).hexdigest()
        self.asset = {"url": "https://github.com/RF-YVY/CyberLabLog/releases/download/v99.0/CyberLab.exe",
                      "version": "v99.0", "digest": f"sha256:{self.sha}", "size": len(self.content), "update_available": True}
        self.root_patch = patch.object(main, "PROJECT_ROOT", self.root)
        self.root_patch.start()
        self.old_shutdown = main.SHUTDOWN_REQUESTED
        main.SHUTDOWN_REQUESTED = False
        main._set_update_state(phase="idle", downloaded_bytes=0, total_bytes=0, message="")

    def tearDown(self):
        self.root_patch.stop()
        main.SHUTDOWN_REQUESTED = self.old_shutdown
        main._set_update_state(phase="idle")
        self.temp.cleanup()

    def response(self, content):
        response = MagicMock()
        response.__enter__.return_value = io.BytesIO(content)
        return response

    def test_verified_download_and_progress(self):
        progress = []
        with patch.object(main.urllib.request, "urlopen", return_value=self.response(self.content)):
            result = main._download_release_asset(self.asset, lambda done, total: progress.append((done, total)))
        self.assertEqual(Path(result["path"]).read_bytes(), self.content)
        self.assertTrue(result["verified"])
        self.assertEqual(progress[-1], (len(self.content), len(self.content)))

    def test_invalid_downloads_never_replace_saved_update(self):
        target = self.root / "CyberLab Case Tracker Update v99.0.exe"
        target.write_bytes(b"previous good download")
        for data in (self.content[:-1], b"MZ" + b"x" * (len(self.content) - 2), self.content + b"extra"):
            with patch.object(main.urllib.request, "urlopen", return_value=self.response(data)):
                with self.assertRaises(ValueError):
                    main._download_release_asset(self.asset)
            self.assertEqual(target.read_bytes(), b"previous good download")
            self.assertFalse(list(self.root.glob("*.download")))

    def test_missing_digest_and_untrusted_url_are_rejected(self):
        for override in ({"digest": ""}, {"url": "https://example.com/update.exe"}):
            with patch.object(main.urllib.request, "urlopen") as request:
                with self.assertRaises(ValueError):
                    main._download_release_asset({**self.asset, **override})
                request.assert_not_called()

    def test_non_executable_is_rejected_even_with_matching_digest(self):
        data = b"not an executable"
        with patch.object(main.urllib.request, "urlopen", return_value=self.response(data)):
            with self.assertRaisesRegex(ValueError, "Windows executable"):
                main._download_release_asset({**self.asset, "size": len(data), "digest": "sha256:" + hashlib.sha256(data).hexdigest()})

    def test_environment_is_independent(self):
        with patch.dict(os.environ, {"_PYI_APPLICATION_HOME_DIR": "gone", "_MEIPASS2": "gone", "CYBERLAB_APP_ROOT": "preserved", "PSModulePath": "powershell7-only"}):
            env = update_installer.installer_environment()
            self.assertFalse(any(key.startswith(("_PYI_", "_MEIPASS")) for key in env))
            self.assertEqual(env["PYINSTALLER_RESET_ENVIRONMENT"], "1")
            self.assertEqual(env["CYBERLAB_APP_ROOT"], "preserved")
            self.assertIn("_MEIPASS2", os.environ)
            self.assertNotIn("PSModulePath", env)

    def test_handoff_verifies_file_and_quotes_paths(self):
        staged = self.root / "O'Brien update.exe"
        target = self.root / "app.exe"
        staged.write_bytes(self.content)
        target.write_bytes(b"old")
        with patch.object(update_installer.subprocess, "Popen") as spawn:
            update_installer.launch_handoff(staged, target, self.sha, 12345)
            script = base64.b64decode(spawn.call_args.args[0][-1]).decode("utf-16le")
            self.assertIn("O''Brien update.exe", script)
            self.assertIn("Wait-Process -Id 12345", script)
            self.assertIn("ComputeHash", script)
            self.assertIn(".previous", script)
            self.assertEqual(spawn.call_args.kwargs["env"]["PYINSTALLER_RESET_ENVIRONMENT"], "1")
            spawn.reset_mock()
            staged.write_bytes(b"MZ tampered")
            with self.assertRaisesRegex(ValueError, "checksum"):
                update_installer.launch_handoff(staged, target, self.sha, 12345)
            spawn.assert_not_called()

    def test_duplicate_install_is_rejected_and_failure_is_retryable(self):
        with patch.object(main.sys, "frozen", True, create=True), patch.object(main.threading, "Thread") as worker:
            self.assertTrue(main.install_portable_update()["ok"])
            with self.assertRaises(HTTPException) as error:
                main.install_portable_update()
            self.assertEqual(error.exception.status_code, 409)
            main._set_update_state(phase="failed")
            self.assertTrue(main.install_portable_update()["ok"])
            self.assertEqual(worker.return_value.start.call_count, 2)

    def test_worker_backs_up_before_shutdown_and_stays_open_on_failure(self):
        database = self.root / "cases.db"
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute("CREATE TABLE test_data (value TEXT)")
            connection.execute("INSERT INTO test_data VALUES ('preserved')")
        staged = self.root / "update.exe"
        staged.write_bytes(self.content)
        with patch.object(main, "_latest_release_asset", return_value=self.asset), \
             patch.object(main, "_download_release_asset", return_value={"path": str(staged), "sha256": self.sha}), \
             patch.object(main, "active_db_path", return_value=database), \
             patch.object(main, "backup_dir", return_value=self.root), \
             patch.object(main, "record_audit"), patch.object(main.time, "sleep"), \
             patch.object(main, "launch_handoff") as launch:
            main._install_update_worker()
            self.assertTrue(main.SHUTDOWN_REQUESTED)
            backup = next(self.root.glob("pre_update_*.db"))
            with closing(sqlite3.connect(backup)) as connection:
                self.assertEqual(connection.execute("SELECT value FROM test_data").fetchone()[0], "preserved")
            main.SHUTDOWN_REQUESTED = False
            launch.side_effect = OSError("helper failed")
            main._install_update_worker()
            self.assertFalse(main.SHUTDOWN_REQUESTED)
            self.assertEqual(main.update_install_status()["phase"], "failed")


if __name__ == "__main__":
    unittest.main()
