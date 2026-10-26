# CyberLab Case Tracker v3.0.12

Released October 26, 2026.

## Install updates from CyberLab

In the packaged Windows app, open **Settings → Portable Application Update**, select **Check**, then **Download & Install**. The app shows download progress, verifies GitHub's SHA-256 digest and the complete asset size, saves a database backup, closes, replaces the executable, and reopens automatically.

The update helper runs independently of PyInstaller's temporary extraction directory, retries locked-file replacement, retains the old executable as `.exe.previous`, and restores it if replacement or launch fails. Cases, settings, report assets, and custom backgrounds remain in their existing locations. Installation results are recorded in `app_data/update-install.log`. **Download EXE only** is also available.

**Upgrading from v3.0.11 or earlier:** those versions can download the new EXE but do not yet have the install action. Close the old app and replace its EXE with this release once, retaining the existing filename and leaving `caselog_gui_v6.db` and `app_data` beside it. Future releases can then be installed from inside the GUI. No administrator installation is required in a writable application folder.

## Custom backgrounds and grouped themes

- Load a PNG, JPG, or WebP background in **Settings → Appearance**.
- The **Show Custom Background** switch hides the image without deleting it and remembers the setting across launches.
- Theme choices are organized into Glass & Future, Cyber & Future, Fun & Colorful, Fun & Future, and Cyber & Fun categories, alongside everyday and classic themes.
- New themes: Frosted Lab, Bento Pop, Bento Mint, Aurora Lab, Citrus Circuit, and Coral Console.
- Aurora motion respects the Interface Animations switch and reduced-motion preferences.

## Readable graph reports

- Full vector PDF charts paginate at up to 20 categories per landscape page, with fewer entries for wrapped names.
- Every PDF page uses the same axis scale and retains readable label sizes.
- PNGs provide a labeled overview when there are too many categories for one page. PDFs and CSVs retain all categories.

## Validation

- 31 backend regression tests and 3 frontend tests pass.
- Production frontend build and Windows single-file EXE build.
- Real Windows handoff from v3.0.11 to v3.0.12: old process exit, verified replacement, automatic restart, previous-EXE retention, and database/user-asset preservation.
- Background hide/show persistence verified in the browser with an isolated database.

Download the Windows EXE and optional `.sha256` checksum file below. Application data is not bundled in the release executable.
