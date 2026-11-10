import base64
from io import BytesIO
import json
import unittest
from unittest.mock import patch

from PIL import Image

import native_label_print as printing
import main


class NativeLabelPrintTests(unittest.TestCase):
    def test_dymo_feed_geometry_and_other_printers(self):
        self.assertEqual(printing.native_geometry(4, 2.3125, "DYMO LabelWriter 550"), {"paper_width": 231, "paper_height": 400, "rotate": True})
        self.assertEqual(printing.native_geometry(2.3125, 4, "DYMO LabelWriter 550")["rotate"], False)
        self.assertEqual(printing.native_geometry(4, 2.3125, "Other printer"), {"paper_width": 400, "paper_height": 231, "rotate": False})
        with self.assertRaises(ValueError):
            printing.native_geometry(float("nan"), 2, "Dymo")

    def test_direct_job_rotates_image_and_uses_one_windows_process(self):
        image = Image.new("RGB", (1200, 694), "white")
        image.putpixel((0, 0), (255, 0, 0))
        stream = BytesIO(); image.save(stream, "PNG")
        data = "data:image/png;base64," + base64.b64encode(stream.getvalue()).decode()
        def spool(script, job, timeout):
            manifest = json.loads(job.read_text(encoding="utf-8"))
            self.assertEqual((manifest["paper_width"], manifest["paper_height"]), (231, 400))
            self.assertEqual(len(manifest["images"]), 2)
            with Image.open(manifest["images"][0]) as rotated:
                self.assertEqual(rotated.size, (694, 1200))
                self.assertEqual(rotated.getpixel((0, 1199)), (255, 0, 0))
            self.assertEqual(timeout, 120)
            return {"success": True, "count": 2, "printer": manifest["printer"]}
        with patch.object(printing, "_powershell", side_effect=spool) as run:
            result = printing.print_images([data, data], 4, 2.3125, "DYMO LabelWriter 550")
            self.assertEqual(result["count"], 2)
            run.assert_called_once()

    def test_bad_jobs_are_rejected_before_spooling(self):
        with patch.object(printing, "_powershell") as run:
            for images, printer in [([], "Dymo"), (["https://example.com/image"], "Dymo"), (["data"], "")]:
                with self.assertRaises(ValueError):
                    printing.print_images(images, 4, 2.3125, printer)
            run.assert_not_called()

    def test_printer_discovery_prefers_dymo_550_and_api_forwards_selected_printer(self):
        with patch.object(printing.sys, "platform", "win32"), patch.object(printing, "_powershell", return_value={"printers": ["DYMO 450", "DYMO LabelWriter 550", "Office"], "default": "Office"}):
            self.assertEqual(printing.list_printers()["recommended"], "DYMO LabelWriter 550")
        with patch.object(printing, "print_images", return_value={"success": True, "count": 1}) as print_job:
            main.print_labels_direct(main.NativeLabelPrintPayload(images=["image"], width=4, height=2.3125, printer_name="DYMO LabelWriter 550"))
            print_job.assert_called_once_with(["image"], 4, 2.3125, "DYMO LabelWriter 550")
