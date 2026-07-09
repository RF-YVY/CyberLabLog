# CyberLab Modernization

This folder is the migration path from the legacy Tkinter desktop app to a modern
browser UI with a Python backend.

The migration goals are:

- Preserve the existing `caselog_gui_v6.db` SQLite database.
- Preserve the existing `app_data/` folder, including report settings, logos,
  marker icons, backups, and automated report outputs.
- Keep automated report, graph, and map exports working while the new UI is
  rebuilt.
- Allow users to either place the legacy database/app data in the app folder or
  import them from inside the new application.

## Architecture

- `backend/`: FastAPI service that reads the legacy database structure, imports
  old data, and bridges to the existing automated export engine.
- `frontend/`: React/Vite browser UI shell. This is where the modern rounded,
  responsive interface will live.

The first phase intentionally keeps the legacy export engine available through a
backend subprocess call:

```powershell
python CyberLabCaseTracker.py --run-automated-exports
```

That lets the new app modernize the interface without risking loss of the
existing report and graph export behavior.

## Development

Backend:

```powershell
cd modern_app\backend
python -m pip install -r requirements.txt
python -m uvicorn main:app --reload --port 8768
```

Frontend:

```powershell
cd modern_app\frontend
npm install
npm run dev
```

The frontend expects the backend at `http://127.0.0.1:8768` unless
`VITE_API_BASE` is set.

## Local App Launch

For migration testing closer to the final packaged application, build the React
UI and let FastAPI serve both the API and the frontend from one local address:

```powershell
cd modern_app\frontend
npm run build
cd ..
python launch_modern_app.py
```

Or use the Windows wrapper from the project root:

```powershell
.\modern_app\run_app.ps1
```

The application opens at `http://127.0.0.1:8768`. This is the preferred path for
packaging because the browser UI and backend share one local server.

## Windows Build

The current packaging scaffold builds the frontend, bundles the FastAPI backend,
and creates a Windows app folder:

```powershell
.\modern_app\build_windows.ps1
```

The output is written to:

```text
dist\CyberLab Case Tracker\CyberLab Case Tracker.exe
```

For packaged testing, place `caselog_gui_v6.db` and `app_data\` beside the EXE
folder contents, or import them from Settings inside the app. In development,
the app continues to use the repository-root database and `app_data` folder.

## Data Compatibility

The backend uses the same project-root data layout as the legacy app:

- `caselog_gui_v6.db`
- `app_data/`
- `app_data/backups/`
- `app_data/automated_reports/`

Import options are exposed by API and will be surfaced in the UI:

- Upload a legacy `.db` file to replace the active database after backing up the
  current one.
- Upload a zipped `app_data` folder and merge it into the active `app_data`.
