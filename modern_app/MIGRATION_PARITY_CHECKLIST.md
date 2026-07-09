# CyberLab Case Tracker Modern Migration Checklist

This checklist tracks the browser UI migration against the legacy desktop application.

## Completed

- Legacy SQLite database can be used in place or imported from Settings.
- Legacy `app_data` can be imported as a ZIP from Settings.
- Completed case table supports search, sorting, view, edit, duplicate, and delete.
- New case and in-progress case entry use dropdown values for repeated fields.
- Last-used Examiner, Investigator, Agency, City, State, Offense Type, and Device Type are retained after saving.
- In-progress cases can be edited, duplicated, deleted, and completed into the main case table.
- Report logo can be selected by the user and is reused in the header and report exports.
- Custom map marker images can be uploaded as BMP, JPG, or PNG and are normalized to transparent PNG.
- Map marker size responds to zoom level.
- Map default focus can be selected from Settings and from the Map view.
- Native export engine generates PDF, XLSX, CSV, graph, and map output without depending on the legacy GUI.
- Analytics preview supports multiple graph groups while keeping a four-card layout.
- Theme library includes Cyber Blue plus the additional visual themes from the migration pass.
- Application profile stores Agency/Organization and Name for UI and reports.
- Header displays the app version and checks GitHub releases for newer versions.
- Browser close requests backend shutdown so the packaged app does not leave ports occupied.
- Packaged build defaults to a windowed user build; debug console builds are available with `build_windows.ps1 -Debug`.
- Settings includes manual backup, restore, and support bundle creation.

## Needs Validation

- Compare exported reports against representative legacy exports for field coverage, logo placement, and graph accuracy.
- Verify map focus choices with real case distributions outside Mississippi.
- Verify first-launch browser behavior on clean Windows profiles and systems with different default browsers.
- Test backup restore with a copied database after a large data-entry session.
- Test update badge behavior after the next GitHub release is published.

## Remaining Migration Work

- Add scheduled automated report execution in the modern runtime if users still need unattended exports.
- Add a guided first-run/import wizard for users who do not know where their legacy database and `app_data` folder are located.
- Add richer report preview inside the browser before export.
- Add dropdown value editing/removal, not just adding new values.
- Add optional password or local access controls if the modern browser runtime will be used on shared lab machines.
- Add packaged installer polish: Start Menu shortcut, desktop shortcut, versioned installer, and uninstall metadata.
- Build a repeatable regression test pack with sample legacy data, expected export files, and screenshot checks.
