"""Exercise a packaged EXE against disposable data, never the user's case folder."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def check_release(executable, version):
    with tempfile.TemporaryDirectory(prefix="cyberlab-release-smoke-") as directory:
        root = Path(directory).resolve()
        exe = root / "CyberLab Case Tracker.exe"
        shutil.copy2(executable, exe)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        def api(path, payload=None, method=None):
            data = json.dumps(payload).encode() if payload is not None else None
            request = Request(base + path, data=data, method=method, headers={"Content-Type": "application/json"})
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        environment = os.environ.copy()
        environment.pop("CYBERLAB_APP_ROOT", None)
        process = subprocess.Popen([str(exe), "--no-browser", "--port", str(port)], cwd=root, env=environment, creationflags=subprocess.CREATE_NO_WINDOW)
        ready = False
        try:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"Packaged app exited early: {process.returncode}")
                try:
                    health = api("/api/health")
                    ready = True
                    break
                except OSError:
                    time.sleep(0.5)
            if not ready:
                raise RuntimeError("Packaged app did not start")
            assert Path(health["database"]).parent.resolve() == root
            assert api("/openapi.json")["info"]["version"] == version
            assert "printers" in api("/api/evidence-labels/printers")
            try:
                api("/api/evidence-labels/print-direct", {"images": [], "width": 4, "height": 2.3125, "printer_name": ""})
                raise AssertionError("An unselected printer must be rejected")
            except HTTPError as error:
                assert error.code == 400
            row = api("/api/in-progress", {"case_number": "TEST-26-1234-1", "city_of_offense": "Oxford", "state_of_offense": "MS", "latitude": 34.366, "longitude": -89.519, "notes": "Disposable release test"})
            label = {"values": {"case": row["case_number"], "serial": "SMOKE-SERIAL", "exhibit": "1"}, "extra": [{"label": "Shelf", "value": "Test"}], "barcode": row["case_number"]}
            api(f"/api/in-progress/{row['id']}/evidence-label", label, "PUT")
            saved = api(f"/api/in-progress/{row['id']}")
            assert json.loads(saved["evidence_label"]) == label
            completed = api(f"/api/in-progress/{row['id']}/complete", method="POST")
            assert completed["case_number"] == row["case_number"]
            assert json.loads(completed["evidence_label"]) == label
            image = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4//8/AAX+Av4N70a4AAAAAElFTkSuQmCC"
            preview = api("/api/evidence-labels/print-pdf", {"images": [image, image], "width": 4, "height": 2.3125})
            with urlopen(preview["url"], timeout=15) as response:
                assert response.headers.get_content_type() == "application/pdf"
                assert response.headers["Content-Disposition"].startswith("inline;")
                assert response.read(5) == b"%PDF-"
            result = api("/api/native-exports/run", {"value": {"output_dir": str(root / "reports"), "report_types": ["map_html"], "map_settings": {"include_completed": True, "include_in_progress": True}}})
            report = next(item for item in result["files"] if item["name"] == "areas_served_map.html")
            content = Path(report["path"]).read_text(encoding="utf-8")
            assert "Custom county map" in content and "Lafayette County" in content
            assert "tile.openstreetmap.org" not in content
            with urlopen(base + "/api/reports/map?" + urlencode({"path": report["path"]}), timeout=30) as response:
                assert "Custom county map" in response.read().decode()
            print(f"PASS: Windows EXE {version}; printer discovery; direct-print validation (no physical job); isolated data; saved labels; HTTP label PDF; completion; bundled county map; report viewer")
        finally:
            if ready:
                try:
                    api("/api/runtime/shutdown", {}, "POST")
                    process.wait(timeout=30)
                except (OSError, subprocess.TimeoutExpired):
                    pass
            if process.poll() is None:
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True, check=False)
                process.wait(timeout=10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("executable", type=Path)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    check_release(args.executable.resolve(), args.version)
