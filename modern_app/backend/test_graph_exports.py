import tempfile
import unittest
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import native_exports as exports


class GraphExportTests(unittest.TestCase):
    def test_pagination_preserves_all_categories_in_order(self):
        for size in (1, 20, 21, 95, 500):
            rows = [{"label": f"Investigator {i}", "value": size - i} for i in range(size)]
            chunks = exports._graph_chunks(rows)
            self.assertEqual([row for chunk in chunks for row in chunk], rows)
            self.assertTrue(all(len(chunk) <= 20 for chunk in chunks))

    def test_long_labels_reduce_page_capacity(self):
        rows = [{"label": "University Police Department - Example County Office " * 2, "value": 1}] * 40
        chunks = exports._graph_chunks(rows)
        self.assertLess(len(chunks[0]), 20)
        self.assertEqual(sum(map(len, chunks)), 40)

    def test_shared_scale_and_distinct_duplicate_labels(self):
        rows = [{"label": "Same name", "value": 1}, {"label": "Same name", "value": 2}]
        fig = exports._graph_figure("Agency", rows, title="Agency", maximum=100)
        try:
            self.assertEqual(fig.axes[0].get_xlim(), (0, 122))
            self.assertEqual(len(set(bar.get_y() for bar in fig.axes[0].patches)), 2)
            self.assertTrue(all(label.get_fontsize() >= 10 for label in fig.axes[0].get_yticklabels()))
        finally:
            plt.close(fig)

    def test_pdf_contains_every_category(self):
        try:
            from pypdf import PdfReader
        except ImportError:
            self.skipTest("pypdf is needed to inspect PDF text")
        rows = [{"label": f"Investigator {i:03}", "value": 100 - i} for i in range(95)]
        with tempfile.TemporaryDirectory() as directory:
            result = exports._write_graph_pdf(Path(directory), "Investigator", "test", rows)
            pdf = PdfReader(result["path"])
            self.assertEqual(len(pdf.pages), 5)
            text = "\n".join(page.extract_text() for page in pdf.pages)
            for row in rows:
                self.assertEqual(text.count(row["label"]), 1)
            self.assertEqual(len(plt.get_fignums()), 0)


if __name__ == "__main__":
    unittest.main()
