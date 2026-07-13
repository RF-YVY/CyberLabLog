from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import database
import cyberlab_workflow as workflow
import family_report
import portable_backup
import custom_report


class WorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_active_db_path = database.active_db_path
        database.active_db_path = lambda: Path(self.temp_dir.name) / "test.db"
        workflow.ensure_workflow_schema()

    def tearDown(self) -> None:
        database.active_db_path = self.original_active_db_path
        self.temp_dir.cleanup()

    def create_case(self, case_number: str, *, progress: bool = False, examiner: str = "Examiner One") -> dict:
        return database.create_case(
            {
                "case_number": case_number,
                "examiner": examiner,
                "investigator": "Investigator One",
                "agency": "Test Agency",
                "city_of_offense": "Oxford",
                "state_of_offense": "MS",
                "start_date": "2026-07-01",
                "end_date": "" if progress else "2026-07-03",
                "volume_size_gb": 64,
                "offense_type": "Fraud",
                "device_type": "iOS",
                "priority": "High",
                "target_due_date": "2026-07-10" if progress else "",
            },
            in_progress=progress,
        )

    def test_case_family_and_next_device_number(self) -> None:
        self.create_case("CC-26-1234-1")
        self.create_case("CC-26-1234-2", progress=True)

        family = workflow.get_case_family("CC-26-1234-1")

        self.assertEqual(family["base_case_number"], "CC-26-1234")
        self.assertEqual(family["device_count"], 2)
        self.assertEqual(family["completed_count"], 1)
        self.assertEqual(family["active_count"], 1)
        self.assertEqual(workflow.next_subcase_number("CC-26-1234-1"), "CC-26-1234-3")

    def test_template_evidence_custody_and_audit_round_trip(self) -> None:
        template = workflow.save_template({"name": "Phone Intake", "payload": {"agency": "Test Agency"}})
        evidence = workflow.save_evidence({"case_number": "CC-26-1234-1", "evidence_number": "E-1", "item_type": "Phone"})
        event = workflow.add_custody_event(evidence["id"], {"event_type": "Received", "person": "Examiner One"})
        workflow.record_audit("case", 1, "created", "CC-26-1234-1", "Case created", {"case_number": {"after": "CC-26-1234-1"}})

        self.assertEqual(template["payload"]["agency"], "Test Agency")
        self.assertEqual(event["event_type"], "Received")
        self.assertEqual(workflow.list_evidence("CC-26-1234-1")[0]["custody_events"][0]["person"], "Examiner One")
        self.assertEqual(workflow.list_audit("CC-26-1234-1", 10)[0]["action"], "created")

    def test_dashboard_and_data_quality(self) -> None:
        self.create_case("CC-26-2000-1")
        self.create_case("CC-26-2000-2", progress=True)
        self.create_case("CC-26-2001", examiner="")

        dashboard = workflow.dashboard_summary()
        quality = workflow.data_quality_summary()

        self.assertEqual(dashboard["family_count"], 2)
        self.assertEqual(dashboard["average_turnaround_days"], 2.0)
        self.assertGreaterEqual(quality["issue_count"], 1)
        self.assertTrue(any("examiner" in issue["message"] for issue in quality["issues"]))

        initial_count = quality["issue_count"]
        dismissed = workflow.dismiss_data_quality_issues([quality["issues"][0]["fingerprint"]])
        self.assertEqual(dismissed["issue_count"], initial_count - 1)
        self.assertEqual(dismissed["dismissed_count"], 1)
        restored = workflow.restore_data_quality_issues()
        self.assertEqual(restored["issue_count"], initial_count)
        self.assertEqual(restored["dismissed_count"], 0)

    def test_review_normalization_duplicate_merge_and_work_queue(self) -> None:
        first = self.create_case("CC-26-3000-1", examiner="ios")
        second = self.create_case("CC-26-3000-1", progress=True, examiner="iOS")
        workflow.save_evidence({"case_number": "CC-26-3000-1", "evidence_number": "E-1", "item_type": "Phone"})

        normalized = workflow.normalize_case_value("examiner", ["ios", "iOS"], "iOS")
        self.assertEqual(normalized["changed"], 2)
        queue = workflow.work_queue_rows()
        self.assertEqual(queue[0]["evidence_count"], 1)

        merged = workflow.merge_duplicate_cases("CC-26-3000-1", "completed", first["id"])
        self.assertEqual(merged["removed"], 1)
        self.assertEqual(merged["kept"]["examiner"], "iOS")
        self.assertEqual(len(workflow.get_case_family("CC-26-3000-1")["members"]), 1)

    def test_case_family_pdf(self) -> None:
        self.create_case("CC-26-4000-1")
        self.create_case("CC-26-4000-2", progress=True)
        workflow.save_evidence({"case_number": "CC-26-4000-1", "evidence_number": "E-1", "item_type": "Phone"})
        target = Path(self.temp_dir.name) / "family.pdf"
        original_logo_path = family_report.logo_path
        family_report.logo_path = lambda: Path(self.temp_dir.name) / "missing-logo.png"
        try:
            family_report.generate_case_family_pdf("CC-26-4000", target)
        finally:
            family_report.logo_path = original_logo_path
        self.assertTrue(target.read_bytes().startswith(b"%PDF"))
        self.assertGreater(target.stat().st_size, 1000)

    def test_encrypted_portable_backup_round_trip(self) -> None:
        self.create_case("CC-26-5000-1")
        root = Path(self.temp_dir.name)
        app_data = root / "app_data"
        backups = app_data / "backups"
        app_data.mkdir(exist_ok=True)
        backups.mkdir(exist_ok=True)
        (app_data / "logo.png").write_bytes(b"test-logo")
        originals = (portable_backup.active_db_path, portable_backup.data_dir, portable_backup.backup_dir)
        portable_backup.active_db_path = database.active_db_path
        portable_backup.data_dir = lambda: app_data
        portable_backup.backup_dir = lambda: backups
        try:
            backup = portable_backup.create_encrypted_backup("correct horse battery staple")
            database.delete_case(1, in_progress=False)
            (app_data / "logo.png").write_bytes(b"changed")
            result = portable_backup.restore_encrypted_backup(backup, "correct horse battery staple")
        finally:
            portable_backup.active_db_path, portable_backup.data_dir, portable_backup.backup_dir = originals
        self.assertGreaterEqual(result["restored_files"], 2)
        self.assertEqual(database.list_cases()["rows"][0]["case_number"], "CC-26-5000-1")
        self.assertEqual((app_data / "logo.png").read_bytes(), b"test-logo")

    def test_custom_monthly_report_filters_and_exports(self) -> None:
        self.create_case("CC-26-6000-1", examiner="Examiner One")
        self.create_case("CC-26-6000-2", examiner="Examiner One")
        self.create_case("CC-26-6001-1", examiner="Examiner Two")
        data = custom_report.custom_report_data("2026-07-01", "2026-07-31", "examiner", "Examiner One")
        self.assertEqual(data["device_count"], 2)
        self.assertEqual(data["total_volume_gb"], 128)
        self.assertEqual(data["device_types"], [("iOS", 2)])
        pdf = Path(self.temp_dir.name) / "monthly.pdf"
        csv_path = Path(self.temp_dir.name) / "monthly.csv"
        original_logo_path = custom_report.logo_path
        custom_report.logo_path = lambda: Path(self.temp_dir.name) / "missing-logo.png"
        try:
            custom_report.generate_custom_pdf(data, pdf)
            custom_report.generate_custom_csv(data, csv_path)
        finally:
            custom_report.logo_path = original_logo_path
        self.assertTrue(pdf.read_bytes().startswith(b"%PDF"))
        self.assertIn("CC-26-6000-1", csv_path.read_text(encoding="utf-8-sig"))


if __name__ == "__main__":
    unittest.main()
