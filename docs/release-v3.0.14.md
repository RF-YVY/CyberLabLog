# CyberLab Case Tracker v3.0.14

Fixes case workspace buttons that appear to do nothing and case-family PDFs failing with **127.0.0.1 refused to connect** after a page reload or navigation.

- **View**, **Edit**, and next-evidence actions now fetch the exact completed/in-progress record by ID. Previously a valid item could be absent from the first 100 search matches.
- Opening errors now appear inside the workspace, where they remain visible.
- Reloads and navigation no longer immediately shut down the local backend. The app tracks each open app page and waits 15 seconds after the last page closes; a reconnect cancels shutdown.
- Closing one app tab does not stop another. Stale close signals from an earlier application instance cannot terminate an active restarted app. The updater's explicit shutdown remains supported.

Validation: 41 backend tests and 4 frontend tests; browser checks for completed View/Edit, active Edit, reload survival, and valid case-family PDF retrieval using isolated test data, including a target beyond 100 matching search results. Packaged EXE checks cover saved edits, family PDFs, reconnect survival, and shutdown after the last page closes.

Update from **Settings → Portable Application Update → Check → Download & Install**. If the local server has already stopped, close the old app tabs and relaunch the EXE first. Existing cases and settings remain in place.
