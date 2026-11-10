import base64
from io import BytesIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from PIL import Image
from pypdf import PdfReader
from starlette.requests import Request
from fastapi import HTTPException

import label_printing
import main


class LabelPrintingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = patch.object(label_printing, "data_dir", return_value=Path(self.temp.name))
        self.root.start()
        stream = BytesIO()
        Image.new("RGBA", (1200, 694), (255, 255, 255, 255)).save(stream, "PNG")
        self.image = "data:image/png;base64," + base64.b64encode(stream.getvalue()).decode()

    def tearDown(self):
        self.root.stop()
        self.temp.cleanup()

    def test_exact_size_multiple_labels_have_no_headers_or_footers(self):
        token = label_printing.create_print_pdf([self.image, self.image], 4, 2.3125)
        pdf = PdfReader(label_printing.preview_path(token))
        self.assertEqual(len(pdf.pages), 2)
        for page in pdf.pages:
            self.assertAlmostEqual(float(page.mediabox.width), 288)
            self.assertAlmostEqual(float(page.mediabox.height), 166.5)
            self.assertEqual(page.extract_text().strip(), "")
            self.assertTrue(page.images)

    def test_http_preview_and_download_use_pdf_content_types(self):
        request = Request({"type": "http", "scheme": "http", "server": ("127.0.0.1", 8768), "path": "/", "root_path": "", "headers": [], "router": main.app.router})
        result = main.prepare_label_print_pdf(main.LabelPrintPayload(images=[self.image], width=4, height=2.3125), request)
        self.assertTrue(result["url"].startswith("http://127.0.0.1:8768/api/evidence-labels/print-pdf/"))
        token = result["url"].rsplit("/", 1)[1][:-4]
        preview = main.read_label_print_pdf(token)
        self.assertEqual(preview.media_type, "application/pdf")
        self.assertTrue(preview.headers["content-disposition"].startswith("inline;"))
        self.assertTrue(main.read_label_print_pdf(token, download=True).headers["content-disposition"].startswith("attachment;"))

    def test_bad_images_dimensions_and_paths_are_rejected(self):
        for images, width, height in [([], 4, 2), (["https://example.com/image"], 4, 2), ([self.image], float("nan"), 2), ([self.image], 0, 2)]:
            with self.assertRaises(ValueError):
                label_printing.create_print_pdf(images, width, height)
        with self.assertRaises(HTTPException):
            main.read_label_print_pdf("../anything")
