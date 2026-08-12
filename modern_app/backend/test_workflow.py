from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import database
import cyberlab_workflow as workflow
import family_report
import portable_backup
import custom_report
import native_exports
import main as backend_main


class WorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_active_db_path = database.active_db_path
        database.active_db_path = lambda: Path(self.temp_dir.name) / "test.db"
        workflow.ensure_workflow_schema()

    def tearDown(self) -> None:
        database.active_db_path = self.original_active_db_path
        self.temp_dir.cleanup()

    def create_case(
        self,
        case_number: str,
        *,
        progress: bool = False,
        examiner: str = "Examiner One",
        agency: str = "Test Agency",
        offense: str = "Fraud",
        subject: str = "",
    ) -> dict:
        return database.create_case(
            {
                "case_number": case_number,
                "examiner": examiner,
                "investigator": "Investigator One",
                "investigation_subject": subject,
                "agency": agency,
                "city_of_offense": "Oxford",
                "state_of_offense": "MS",
                "start_date": "2026-07-01",
                "end_date": "" if progress else "2026-07-03",
                "volume_size_gb": 64,
                "offense_type": offense,
                "device_type": "iOS",
                "priority": "High",
                "target_due_date": "2026-07-10" if progress else "",
            },
            in_progress=progress,
        )

    def test_case_subject_search_and_alphabetical_sorts(self) -> None:
        self.create_case("CC-26-1000", agency="zeta Agency", offense="Theft", subject="Jordan Smith")
        self.create_case("CC-26-1001", agency="Alpha Agency", offense="arson", subject="Taylor Jones")
        self.create_case("CC-26-1002", agency="beta Agency", offense="Burglary", subject="Morgan Lee")

        agencies = [row["agency"] for row in database.list_cases(sort="agency")["rows"]]
        agencies_desc = [row["agency"] for row in database.list_cases(sort="agency_desc")["rows"]]
        offenses = [row["offense_type"] for row in database.list_cases(sort="offense")["rows"]]
        offenses_desc = [row["offense_type"] for row in database.list_cases(sort="offense_desc")["rows"]]
        matches = database.list_cases(search="jordan smith")["rows"]

        self.assertEqual(agencies, ["Alpha Agency", "beta Agency", "zeta Agency"])
        self.assertEqual(agencies_desc, ["zeta Agency", "beta Agency", "Alpha Agency"])
        self.assertEqual(offenses, ["arson", "Burglary", "Theft"])
        self.assertEqual(offenses_desc, ["Theft", "Burglary", "arson"])
        self.assertEqual([row["case_number"] for row in matches], ["CC-26-1000"])
        self.assertEqual(matches[0]["investigation_subject"], "Jordan Smith")

    def test_combo_values_are_alphabetical_and_case_insensitive(self) -> None:
        self.create_case("CC-26-1010", agency="zeta Agency", offense="Theft")
        self.create_case("CC-26-1011", agency="Alpha Agency", offense="arson")
        self.create_case("CC-26-1012", agency="beta Agency", offense="Burglary")
        database.add_combo_value("agency", "Delta Agency")
        database.add_combo_value("offense_type", "Cybercrime")

        self.assertEqual(
            database.get_combo_values("agency"),
            ["Alpha Agency", "beta Agency", "Delta Agency", "zeta Agency"],
        )
        self.assertEqual(
            database.get_combo_values("offense_type"),
            ["arson", "Burglary", "Cybercrime", "Theft"],
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
        self.assertEqual(data["average_turnaround_days"], 2.0)
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

    def test_native_report_turnaround_uses_scoped_completed_rows(self) -> None:
        rows = [
            {"start_date": "2026-07-01", "end_date": "2026-07-03", "volume_size_gb": 1},
            {"start_date": "2026-07-10", "end_date": "2026-07-16", "volume_size_gb": 1},
            {"start_date": "", "end_date": "2026-07-20", "volume_size_gb": 1},
        ]

        summary = native_exports._summary(rows, [], {"date_range_mode": "current_month"})

        self.assertEqual(summary["average_turnaround_days"], 4.0)

    def test_scheduler_catches_up_after_due_time(self) -> None:
        base = {"enable_schedule": True, "schedule_time": "09:00"}
        self.assertEqual(
            backend_main._schedule_token({**base, "frequency": "daily"}, datetime(2026, 7, 16, 9, 45)),
            "daily:2026-07-16",
        )
        self.assertIsNone(backend_main._schedule_token({**base, "frequency": "daily"}, datetime(2026, 7, 16, 8, 59)))
        self.assertEqual(
            backend_main._schedule_token({**base, "frequency": "weekly", "schedule_weekday": "Thursday"}, datetime(2026, 7, 16, 11, 30)),
            "weekly:2026-07-16",
        )
        self.assertEqual(
            backend_main._schedule_token({**base, "frequency": "weekly", "schedule_weekday": "Thursday"}, datetime(2026, 7, 17, 8, 0)),
            "weekly:2026-07-16",
        )
        self.assertIsNone(
            backend_main._schedule_token({**base, "frequency": "weekly", "schedule_weekday": "Thursday"}, datetime(2026, 7, 15, 18, 0))
        )
        self.assertEqual(
            backend_main._schedule_token({**base, "frequency": "monthly", "schedule_month_day": "15"}, datetime(2026, 7, 16, 8, 0)),
            "monthly:2026-07",
        )

    def test_complete_in_progress_case_saves_final_edits(self) -> None:
        active = self.create_case("CC-26-7000-1", progress=True)
        payload = backend_main.CasePayload(
            case_number="CC-26-7000-1",
            examiner="Examiner One",
            investigator="Investigator One",
            agency="Updated Agency",
            city_of_offense="Oxford",
            state_of_offense="MS",
            start_date="2026-07-01",
            end_date="2026-07-28",
            volume_size_gb=96,
            offense_type="Fraud",
            device_type="Android",
            workflow_status="Ready for Completion",
            priority="High",
        )

        completed = backend_main.complete_case(active["id"], payload)

        self.assertIsNone(database.get_case(active["id"], in_progress=True))
        self.assertEqual(completed["agency"], "Updated Agency")
        self.assertEqual(completed["device_type"], "Android")
        self.assertEqual(completed["volume_size_gb"], 96)


if __name__ == "__main__":
    unittest.main()
