import json
import shutil
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi import HTTPException
import main
import native_exports
import report_basemap


def sample_points():
    return [{"city": "Holly Springs", "state": "MS", "latitude": 34.767,
             "longitude": -89.448, "case_count": 38, "completed_count": 38,
             "in_progress_count": 0, "total_volume_gb": 4590, "cases": []},
            {"city": "Oxford", "state": "MS", "latitude": 34.366,
             "longitude": -89.519, "case_count": 10, "completed_count": 10,
             "in_progress_count": 0, "total_volume_gb": 200, "cases": []}]


class MapExportTests(unittest.TestCase):
    def test_default_generation_is_fully_offline_and_usgs_is_opt_in(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "areas_served_map.html"
            with patch.object(native_exports, "fetch_background", side_effect=AssertionError("Default must not fetch USGS")):
                native_exports._write_map_html(path, sample_points())
            self.assertIn("Custom county map", path.read_text(encoding="utf-8"))
            with patch.object(native_exports, "fetch_background", return_value={}) as fetch:
                native_exports._write_map_html(path, sample_points(), {"include_usgs_background": True})
                fetch.assert_called_once()

    def test_runtime_custom_map_never_requests_tiles_even_with_optional_background(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("Node is needed for the map-script runtime check")
        script = native_exports._map_html_content(sample_points()).rsplit('<script>', 1)[1].split('</script>', 1)[0]
        harness = r'''
const vm = require('node:vm');
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
let tileCalls = 0, currentZoom = 7, fitOptions, tileError;
const layers = new Set();
const status = {textContent: ''};
const chain = () => ({addTo() {return this}, clearLayers() {return this}, bindTooltip() {return this}, bindPopup() {return this}, setIcon() {return this}});
const map = {
  setView() {return this}, getZoom() {return currentZoom},
  setZoom(z) {currentZoom=z; return this}, on() {return this},
  getSize() {return {x: 1000, y: 650}}, latLngToContainerPoint() {return {x: 500, y: 300}},
  fitBounds(bounds, options) {fitOptions=options},
  hasLayer(layer) {return layers.has(layer)}, removeLayer(layer) {layers.delete(layer)},
  attributionControl: {addAttribution() {}}
};
const L = {
  map: () => map, geoJSON: () => chain(),
  layerGroup() {const layer=chain(); layer.addTo=() => {layers.add(layer);return layer};return layer},
  tileLayer() {tileCalls++;const layer=chain();layer.addTo=() => {layers.add(layer);return layer};layer.on=(event, callback)=>{tileError=callback};return layer},
  imageOverlay() {return chain()},
  marker: () => chain(), icon: () => ({}), divIcon: () => ({}),
  control: Object.assign(() => chain(), {scale: () => chain(), layers: () => chain()}),
  DomUtil: {create: () => ({})}
};
vm.runInNewContext(input.script, {L, location: {protocol: input.protocol}, document: {getElementById: () => status}});
if (tileCalls !== 0 || fitOptions.maxZoom !== 10) throw Error('Custom map requested online tiles or lost useful zoom');
console.log('pass');
'''
        for protocol in ("file:", "https:"):
            result = subprocess.run([node, "-e", harness], input=json.dumps({"script": script, "protocol": protocol}), text=True, capture_output=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
            background = {"image": "data:image/jpeg;base64,/9j/", "bounds": [[32, -91], [35, -88]]}
            embedded_script = native_exports._map_html_content(sample_points(), {"background": background}).rsplit('<script>', 1)[1].split('</script>', 1)[0]
            result = subprocess.run([node, "-e", harness], input=json.dumps({"script": embedded_script, "protocol": protocol, "embedded": True}), text=True, capture_output=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_counties_are_bundled_and_filtered_to_the_report_region(self):
        features = report_basemap.county_features(sample_points())["features"]
        names = [f["properties"]["name"] for f in features]
        self.assertIn("Lafayette County", names)
        self.assertIn("Marshall County", names)
        self.assertNotIn("Los Angeles County", names)
        result = native_exports._map_html_content(sample_points())
        self.assertIn("County lines and names", result)
        self.assertIn("Lafayette County", result)

    def test_background_failure_is_nonfatal_and_invalid_snapshots_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(report_basemap, "data_dir", return_value=Path(directory)), patch.object(report_basemap, "_read_url", side_effect=OSError("offline")):
            self.assertEqual(report_basemap.fetch_background(sample_points()), {})
        self.assertEqual(report_basemap.valid_background({"image": "https://example.com/image", "bounds": [[32, -91], [35, -88]]}), {})

    def test_background_is_cached_and_report_viewer_preserves_the_embedded_snapshot(self):
        background = {"image": "data:image/jpeg;base64,/9j/", "bounds": [[32, -91], [35, -88]]}
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "map_backgrounds"
            cache.mkdir()
            # A successful fetch is cached; subsequent offline generation can reuse it.
            metadata = {"href": "https://basemap.nationalmap.gov/arcgis/rest/directories/test.jpg", "extent": {"xmin": -10130173, "ymin": 3763310, "xmax": -9796115, "ymax": 4163881}}
            with patch.object(report_basemap, "data_dir", return_value=Path(directory)), patch.object(report_basemap, "_read_url", side_effect=[json.dumps(metadata).encode(), b"\xff\xd8\xff"]):
                first = report_basemap.fetch_background(sample_points())
                self.assertTrue(first)
            with patch.object(report_basemap, "data_dir", return_value=Path(directory)), patch.object(report_basemap, "_read_url", side_effect=AssertionError("Must use cache")):
                self.assertEqual(report_basemap.fetch_background(sample_points()), first)
            path = Path(directory) / "areas_served_map.html"
            path.write_text(native_exports._map_html_content(sample_points(), {"background": background}), encoding="utf-8")
            response = main.view_exported_map(str(path))
            self.assertIn('"image": "data:image/jpeg;base64,/9j/"', response.body.decode())

    def test_standalone_assets_and_protocol_guard(self):
        result = native_exports._map_html_content(sample_points())
        self.assertNotIn('src="https://unpkg', result)
        self.assertNotIn('href="https://unpkg', result)
        self.assertIn("['http:', 'https:'].includes(location.protocol)", result)
        self.assertNotIn("https://tile.openstreetmap.org/{z}/{x}/{y}.png", result)
        self.assertNotIn("{s}.tile.openstreetmap", result)
        self.assertIn("Custom county map", result)
        self.assertIn("Mississippi", result)
        self.assertIn("city-name-marker", result)
        self.assertIn("Redistribution and use", result)

    def test_report_viewer_preserves_snapshot_but_ignores_file_scripts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "areas_served_map.html"
            data = {"points": sample_points(), "includeCaseDetails": False}
            path.write_text('<script>window.untrustedFileScript = true;</script>\nconst exportData = '+json.dumps(data)+';', encoding="utf-8")
            response = main.view_exported_map(str(path))
            self.assertEqual(response.headers["referrer-policy"], "strict-origin-when-cross-origin")
            output = response.body.decode()
            self.assertIn('"case_count": 38', output)
            self.assertNotIn("untrustedFileScript", output)
            self.assertIn('"includeCaseDetails": false', output)

    def test_invalid_report_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "areas_served_map.html"
            path.write_text("Not a CyberLab report", encoding="utf-8")
            with self.assertRaises(HTTPException):
                main.view_exported_map(str(path))

    def test_empty_and_hostile_text_are_safe(self):
        self.assertIn('"points": []', native_exports._map_html_content([]))
        points = sample_points()
        points[0]["city"] = '</script><img src=x onerror="alert(1)">'
        points[0]["completed_count"] = '<img src=x onerror="alert(1)">'
        result = native_exports._map_html_content(points)
        self.assertNotIn('</script><img src=x', result)
        self.assertIn("escapeText(point.city)", result)
        self.assertNotIn('<td><img', result)
        self.assertIn("escapeText(point.completed_count)", result)


if __name__ == "__main__":
    unittest.main()
