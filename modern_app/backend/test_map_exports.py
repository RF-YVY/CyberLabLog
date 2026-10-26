import json
import shutil
import subprocess
from pathlib import Path
import tempfile
import unittest

from fastapi import HTTPException
import main
import native_exports


def sample_points():
    return [{"city": "Holly Springs", "state": "MS", "latitude": 34.767,
             "longitude": -89.448, "case_count": 38, "completed_count": 38,
             "in_progress_count": 0, "total_volume_gb": 4590, "cases": []},
            {"city": "Oxford", "state": "MS", "latitude": 34.366,
             "longitude": -89.519, "case_count": 10, "completed_count": 10,
             "in_progress_count": 0, "total_volume_gb": 200, "cases": []}]


class MapExportTests(unittest.TestCase):
    def test_runtime_local_file_never_requests_tiles_and_http_falls_back(self):
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
const chain = () => ({addTo() {return this}, bindTooltip() {return this}, bindPopup() {return this}, setIcon() {return this}});
const map = {
  setView() {return this}, getZoom() {return currentZoom},
  setZoom(z) {currentZoom=z; return this}, on() {return this},
  fitBounds(bounds, options) {fitOptions=options},
  hasLayer(layer) {return layers.has(layer)}, removeLayer(layer) {layers.delete(layer)},
  attributionControl: {addAttribution() {}}
};
const L = {
  map: () => map, geoJSON: () => chain(),
  layerGroup() {const layer=chain(); layer.addTo=() => {layers.add(layer);return layer};return layer},
  tileLayer() {tileCalls++;const layer=chain();layer.addTo=() => {layers.add(layer);return layer};layer.on=(event, callback)=>{tileError=callback};return layer},
  marker: () => chain(), icon: () => ({}), divIcon: () => ({}),
  control: Object.assign(() => chain(), {scale: () => chain(), layers: () => chain()}),
  DomUtil: {create: () => ({})}
};
vm.runInNewContext(input.script, {L, location: {protocol: input.protocol}, document: {getElementById: () => status}});
if (input.protocol === 'file:') {
  if (tileCalls !== 0 || fitOptions.maxZoom !== 6) throw Error('Standalone map requested tiles or lost overview zoom');
} else {
  if (tileCalls !== 1) throw Error('HTTP street layer missing');
  currentZoom=12; tileError();
  if (currentZoom !== 6 || !status.textContent.includes('unavailable')) throw Error('Tile failure did not restore overview');
}
console.log('pass');
'''
        for protocol in ("file:", "https:"):
            result = subprocess.run([node, "-e", harness], input=json.dumps({"script": script, "protocol": protocol}), text=True, capture_output=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_standalone_assets_and_protocol_guard(self):
        result = native_exports._map_html_content(sample_points())
        self.assertNotIn('src="https://unpkg', result)
        self.assertNotIn('href="https://unpkg', result)
        self.assertIn("['http:', 'https:'].includes(location.protocol)", result)
        self.assertIn("https://tile.openstreetmap.org/{z}/{x}/{y}.png", result)
        self.assertNotIn("{s}.tile.openstreetmap", result)
        self.assertIn("Geographic overview", result)
        self.assertIn("Mississippi", result)
        self.assertIn("tileerror", result)
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
