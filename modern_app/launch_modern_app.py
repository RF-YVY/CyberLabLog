from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from pathlib import Path

import uvicorn


APP_ROOT = Path(__file__).resolve().parent
if getattr(sys, "frozen", False):
    USER_APP_ROOT = Path(sys.executable).resolve().parent
    RESOURCE_ROOT = Path(getattr(sys, "_MEIPASS", USER_APP_ROOT)).resolve()
else:
    USER_APP_ROOT = APP_ROOT.parent
    RESOURCE_ROOT = APP_ROOT

LOCAL_STATE_ROOT = Path(os.environ.get("LOCALAPPDATA", USER_APP_ROOT)) / "CyberLab Case Tracker"
BROWSER_PROFILE_ROOT = LOCAL_STATE_ROOT / "BrowserProfiles"
BACKEND_ROOT = RESOURCE_ROOT / "backend"
FRONTEND_INDEX = RESOURCE_ROOT / "frontend" / "dist" / "index.html"


def wait_for_port(host: str, port: int, timeout: float = 12.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.4):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def wait_for_http(url: str, timeout: float = 18.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=0.8) as response:
                if 200 <= response.status < 500:
                    return True
        except Exception:
            time.sleep(0.25)
    return False


def port_is_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.4)
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def choose_port(host: str, preferred_port: int, attempts: int = 20) -> int:
    for offset in range(attempts):
        candidate = preferred_port + offset
        if port_is_available(host, candidate):
            return candidate
    raise RuntimeError(f"No open port found from {preferred_port} to {preferred_port + attempts - 1}")


def _edge_candidates() -> list[str | None]:
    return [
        shutil.which("msedge"),
        os.environ.get("PROGRAMFILES", "") + r"\Microsoft\Edge\Application\msedge.exe",
        os.environ.get("PROGRAMFILES(X86)", "") + r"\Microsoft\Edge\Application\msedge.exe",
        os.environ.get("LOCALAPPDATA", "") + r"\Microsoft\Edge\Application\msedge.exe",
    ]


def _chrome_candidates() -> list[str | None]:
    return [
        shutil.which("chrome"),
        shutil.which("chromium"),
        os.environ.get("PROGRAMFILES", "") + r"\Google\Chrome\Application\chrome.exe",
        os.environ.get("PROGRAMFILES(X86)", "") + r"\Google\Chrome\Application\chrome.exe",
        os.environ.get("LOCALAPPDATA", "") + r"\Google\Chrome\Application\chrome.exe",
    ]


def find_browser_executable(preferred_browser: str = "system_default") -> str | None:
    if preferred_browser == "edge":
        candidates = _edge_candidates()
    elif preferred_browser == "chrome":
        candidates = _chrome_candidates()
    else:
        candidates = [*_chrome_candidates(), *_edge_candidates()]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def read_browser_preference() -> str:
    db_path = USER_APP_ROOT / "caselog_gui_v6.db"
    if not db_path.exists():
        return "system_default"
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = ?",
                ("combo_json_browser_preferences",),
            ).fetchone()
        if not row or not row[0]:
            return "system_default"
        outer = json.loads(row[0])
        value = json.loads(outer[0]) if isinstance(outer, list) and outer else {}
        preferred = str(value.get("preferred_browser") or "system_default")
        if preferred in {"system_default", "auto", "edge", "chrome"}:
            return preferred
    except Exception:
        return "system_default"
    return "system_default"


def cleanup_browser_profiles(max_age_seconds: int = 86400) -> None:
    root = BROWSER_PROFILE_ROOT
    if not root.exists():
        return
    cutoff = time.time() - max_age_seconds
    for item in root.iterdir():
        try:
            if item.is_dir() and item.stat().st_mtime < cutoff:
                shutil.rmtree(item, ignore_errors=True)
        except OSError:
            continue


def open_browser(url: str, port: int, preferred_browser: str) -> subprocess.Popen | None:
    # Give the OS a tiny moment to settle after the socket opens.
    time.sleep(0.4)
    launch_url = f"{url}/?cyberlab_port={port}&launch={int(time.time())}"
    if preferred_browser == "system_default":
        webbrowser.open(launch_url)
        return None

    browser = find_browser_executable(preferred_browser)
    if not browser:
        webbrowser.open(launch_url)
        return None

    process = subprocess.Popen(
        [
            browser,
            "--new-window",
            launch_url,
            "--disable-session-crashed-bubble",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return process


def monitor_shutdown_request(server: uvicorn.Server, runtime_module: object) -> None:
    while not server.should_exit:
        shutdown_requested = bool(getattr(runtime_module, "SHUTDOWN_REQUESTED", False))
        if shutdown_requested:
            print("Browser window closed. Shutting down backend server.")
            server.should_exit = True
            break
        time.sleep(1.0)


def launch_and_monitor_browser(server: uvicorn.Server, host: str, port: int, url: str) -> None:
    if not wait_for_port(host, port) or not wait_for_http(url):
        return
    cleanup_browser_profiles()
    browser_session = open_browser(url, port, read_browser_preference())
    if browser_session is None:
        return
    # Edge/Chrome often hand the new window off to an existing browser process and
    # let this starter process exit immediately. Do not treat that as window close.


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch CyberLab Case Tracker modern app.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8768)
    parser.add_argument("--no-browser", action="store_true", help="Start the app without opening a browser.")
    args = parser.parse_args()

    if not FRONTEND_INDEX.exists():
        print(
            "Frontend build not found. Run `npm run build` from modern_app/frontend before launching.",
            file=sys.stderr,
        )
        return 2

    os.environ.setdefault("CYBERLAB_APP_ROOT", str(USER_APP_ROOT))
    sys.path.insert(0, str(BACKEND_ROOT))
    import main as backend_main

    try:
        selected_port = choose_port(args.host, args.port)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 3

    if selected_port != args.port:
        print(f"Port {args.port} is already in use. Starting on port {selected_port} instead.")

    url = f"http://{args.host}:{selected_port}"

    config = uvicorn.Config(
        backend_main.app,
        host=args.host,
        port=selected_port,
        log_level="info",
        log_config=None,
        access_log=False,
    )
    server = uvicorn.Server(config)
    threading.Thread(target=monitor_shutdown_request, args=(server, backend_main), daemon=True).start()
    if not args.no_browser:
        threading.Thread(target=launch_and_monitor_browser, args=(server, args.host, selected_port, url), daemon=True).start()
    server.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
