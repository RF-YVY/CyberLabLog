import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database
import main


class EvidenceLabelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = patch.object(database, "active_db_path", return_value=Path(self.temp.name) / "labels.db")
        self.db.start()
        self.label = {"values": {"case": "CC-26-1234-1", "serial": "SN-123", "description": "", "exhibit": "1"}, "extra": [{"label": "Custodian", "value": "Test"}], "barcode": "CC-26-1234-1"}

    def tearDown(self):
        self.db.stop()
        self.temp.cleanup()

    def test_label_roundtrip_does_not_overwrite_case_and_survives_completion(self):
        row = database.create_case({"case_number": "CC-26-1234-1", "notes": "Original notes"}, in_progress=True)
        main.write_evidence_label("in-progress", row["id"], main.EvidenceLabelPayload(**self.label))
        saved = database.get_case(row["id"], in_progress=True)
        self.assertEqual(json.loads(saved["evidence_label"]), self.label)
        self.assertEqual(saved["notes"], "Original notes")
        database.update_case(row["id"], {"case_number": row["case_number"], "notes": "Edited notes"}, in_progress=True)
        completed = database.complete_in_progress_case(row["id"])
        self.assertEqual(json.loads(completed["evidence_label"]), self.label)
        self.assertEqual(completed["notes"], "Edited notes")

    def test_new_case_label_is_stored_and_duplicate_starts_without_old_label(self):
        row = database.create_case({"case_number": "CC-26-1234-1", "evidence_label": self.label})
        self.assertEqual(json.loads(row["evidence_label"]), self.label)
        duplicate = database.duplicate_case(row["id"])
        self.assertIsNone(duplicate["evidence_label"])
        database.delete_case(row["id"])
        with self.assertRaises(ValueError):
            database.save_evidence_label(row["id"], self.label)

    def test_completed_and_progress_ids_do_not_share_label_data(self):
        completed = database.create_case({"case_number": "CC-26-1234-1"})
        progress = database.create_case({"case_number": "CC-26-1234-2"}, in_progress=True)
        self.assertEqual(completed["id"], progress["id"])
        database.save_evidence_label(completed["id"], self.label)
        self.assertIsNone(database.get_case(progress["id"], in_progress=True)["evidence_label"])


if __name__ == "__main__":
    unittest.main()
