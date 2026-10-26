import asyncio
from io import BytesIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi import HTTPException, UploadFile
from PIL import Image

import database
import main


class BackgroundTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_patch = patch.object(database, "active_db_path", lambda: self.root / "test.db")
        self.image_patch = patch.object(main, "background_image_path", lambda: self.root / "custom_background.png")
        self.db_patch.start()
        self.image_patch.start()
        database.ensure_schema()

    def tearDown(self):
        self.image_patch.stop()
        self.db_patch.stop()
        self.temp.cleanup()

    def upload(self, content):
        return asyncio.run(main.upload_background(UploadFile(filename="background.png", file=BytesIO(content))))

    def image(self, color):
        stream = BytesIO()
        Image.new("RGB", (640, 480), color).save(stream, "PNG")
        return stream.getvalue()

    def test_hide_and_show_keep_saved_image_and_persist(self):
        self.assertFalse(main.read_background_info()["exists"])
        self.assertTrue(self.upload(self.image("navy"))["enabled"])
        path = self.root / "custom_background.png"
        saved = path.read_bytes()
        hidden = main.update_background_preferences(main.BackgroundPreferences(enabled=False))
        self.assertFalse(hidden["enabled"])
        self.assertTrue(hidden["exists"])
        self.assertEqual(path.read_bytes(), saved)
        self.assertFalse(database.get_json_setting("background_preferences", {})["enabled"])
        self.assertTrue(main.update_background_preferences(main.BackgroundPreferences(enabled=True))["enabled"])
        self.assertEqual(path.read_bytes(), saved)

    def test_invalid_replacement_preserves_image_and_hidden_setting(self):
        self.upload(self.image("navy"))
        main.update_background_preferences(main.BackgroundPreferences(enabled=False))
        saved = (self.root / "custom_background.png").read_bytes()
        with self.assertRaises(HTTPException):
            self.upload(b"not an image")
        self.assertEqual((self.root / "custom_background.png").read_bytes(), saved)
        self.assertFalse(main.read_background_info()["enabled"])
        self.upload(self.image("coral"))
        self.assertNotEqual((self.root / "custom_background.png").read_bytes(), saved)
        self.assertTrue(main.read_background_info()["enabled"])

    def test_oversize_upload_is_rejected(self):
        with self.assertRaises(HTTPException):
            self.upload(b"x" * (20 * 1024 * 1024 + 1))
        self.assertFalse(main.read_background_info()["exists"])


if __name__ == "__main__":
    unittest.main()
