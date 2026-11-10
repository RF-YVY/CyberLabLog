# CyberLab Modernization

Current release: **v3.0.17** (November 10, 2026).

Coordinate handling rejects zero/zero, partial, non-finite, and out-of-range pairs as unresolved, while accepting a valid pair containing a single zero. Refresh Missing Locations repairs invalid case pairs using a valid cache or sibling record before requesting a geocoder lookup; invalid caches are retried. Writes match snapshot coordinates to preserve concurrent manual edits. A database backup precedes the refresh operation. Form serialization preserves null/empty GPS fields.

Case workspace actions use direct completed/in-progress record endpoints. Browser pages register per-page sessions; pagehide releases only that page, and the backend waits 15 seconds before exiting when no pages reconnect. Instance tokens prevent stale-page close signals from terminating a restarted app. Explicit updater shutdown remains immediate.

Map reports default to a custom county map: soft land/water colors, thin Census county boundaries, muted county names, selected city names, and case markers. Generation and viewing work offline without an API key. Labels avoid overlaps with each other and markers; zoom reveals additional labels, and hover shows full county names. Reports → Output Options offers an optional USGS detail layer, fetched only when selected and cached under `app_data/map_backgrounds`. Even reports containing USGS imagery open on the calmer county map; switch backgrounds through Layers. Zooming enlarges saved USGS imagery instead of requesting additional tiles. **Open Map Report** preserves the saved case snapshot and optional background. Map reports never request OSM tiles. Arbitrary scripts in report files are not executed by the report viewer.

Packaged Windows builds support **Settings → Portable Application Update → Check → Download & Install**. The update is verified with GitHub's SHA-256 digest and exact asset size before a database backup and independent Windows helper replace the EXE and restart it. The helper clears inherited PyInstaller extraction variables, retries locked-file replacement, keeps `.exe.previous` for recovery, and logs to `app_data/update-install.log`. Failed downloads leave the running app intact and can be retried. A source checkout supports download-only updates.

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
- Geocode new and edited city/state locations, while supporting named landmarks
  and exact GPS coordinates for locations without postal addresses.
- Recheck existing completed and in-progress cases from the Map page; successful
  lookups are cached so future scans only retry unresolved locations.

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

## Case and evidence workflow

Settings → Evidence Label Settings stores a separate agency title, division,
logo, and label stock. The default Dymo 550 shipping-label preset is 4 inches
wide × 2 5/16 inches high (landscape). Custom size remains available, with
independent width and height in inches. In either completed or
in-progress case details, Generate Evidence Barcode opens an editable label
preview. Case number, agency, start date, device type, model, investigation
subject, and description (falling back to case notes) are filled from the case.
EXHIBIT is populated from the final numeric evidence suffix (CC-26-1234-2 → 2)
and remains editable. Add serial number, location, or extra named fields as needed. Blank
fields are omitted. Labels follow the reference layout: a QR code at the upper
left beside centered agency and division titles, a thin divider, a prominent
EXHIBIT row, bold field names on the left, and the logo centered on the right.
The QR code defaults to the case number and can be edited to include an exhibit
identifier. Label edits are saved separately from the case's other fields. The preview uses the actual
configured paper width and height; narrow windows can scroll horizontally.
Agency and division titles each stay on one line and shrink to fit. Field text
shrinks within the fixed label bounds. If it cannot fit at a readable size,
printing and downloading are blocked until text is shortened or fields removed.
Print Label creates an exact-size PDF inside CyberLab and opens it through a normal local HTTP address, with a full-page label image and no headers,
footers, or page margins. Use the PDF viewer's Print button, matching paper size,
and Actual size / 100% scale. Printing uses one flattened label image per page to
prevent field rows from splitting across pages. Download PNG uses fixed pixel
dimensions at 300 pixels per inch (1200 × 694 for the Dymo preset, rounded).
Neither output grows taller as more fields are added.

New Case also provides Generate Evidence Barcode before an exhibit is submitted.
Save Label keeps its fields, extra fields, and QR value in the entry draft;
Save & Finish or Save & Add Next Evidence Item stores them with that exhibit.
Printing and downloading also save the label. The next evidence item starts
with a fresh label. Saved labels remain attached when an in-progress exhibit is
completed, while duplicated exhibits start without the original label data.
Reopen an exhibit's generator to edit its saved label. Agency/division branding,
logo, and paper size continue to use current shared label settings.

Case workspaces and case details provide Print All Labels and Download All PNGs
for every completed and in-progress exhibit in the same case family. Bulk output
loads the latest saved labels, falling back to case data for exhibits without a
label, and saves those defaults. It checks every label for overflow before
starting output. Printing puts each label on its own page; downloading creates
one ZIP containing an individually named PNG for each exhibit.

The Cases page opens to Case workspaces. Select a base case number to view all
its completed and in-progress evidence items, or use the Cases tab for the
completed-item table. Each workspace supports viewing, editing, and preparing
the next evidence item.

In New Case, choose Completed or In-Progress, then use Save & Finish or
Save & Add Next Evidence Item. The latter keeps shared information, suggests
the next available suffix across both lists, and clears device type, model,
OS, volume, recovery results, and FPR completion. Saved items appear above the
entry form with edit actions. Shared dates, notes, and custom fields carry over;
review these if they differ for the next item.

Draft feedback appears above the form. Leaving an unfinished entry keeps its
draft; returning after a reload offers recovery. Choosing to replace an entry
for editing requires confirmation. Table Columns and Row spacing settings are
remembered per browser. Select Lab Light or Lab Dark in Settings for the refined
professional themes.

Run frontend regression checks with `npm test` from `frontend/`.

Build the portable Windows EXE with `modern_app/build_windows.ps1`. Add
`-Installer` to also compile the per-user installer using Inno Setup 6 or 7.
Installer sources are in `modern_app/installer.iss`; neither package includes
user databases, uploaded images, or generated reports. Installation targets a
writable per-user folder so the existing portable storage/update behavior works.

## Development commands

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
and creates a single-file Windows executable:

```powershell
.\modern_app\build_windows.ps1
```

The output is written to:

```text
dist\CyberLab Case Tracker.exe
```

For packaged testing, place `caselog_gui_v6.db` and `app_data\` beside the EXE,
or import them from Settings inside the app. In development,
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

Label printing uses ordinary HTTP PDF files for office-browser compatibility. Preparation has timeouts and visible errors, and the print tab supplies open/download links if automatic navigation is blocked. Generated PDFs are local to app_data/label_print_previews and expire from disk after 24 hours when new previews are prepared.
