# Changelog

All notable changes to this project will be documented in this file.

## [3.0.0] - 2025-11-18
- Introduced automated reporting with configurable schedules and date scopes (week, month, or all cases).
- Added dual-scope graph exports so each run produces full-history and date-filtered charts with clear filenames.
- Ensured automated HTML map exports always include every case for consistent sharing.
- Refreshed About tab content to document new workflows and spotlight remote-sharing scenarios.
- Bumped in-app version metadata and rebuilt the Windows executable.

## [2.1.6] - 2025-10-23
- Sorted case aging alerts by urgency and improved spacing in the Total Summary PDF.
- Added safe handling for empty datasets in pie charts and enforced circular rendering.
- Updated embedded application metadata (version/date) ahead of the 2.1.6 build.
- Confirmed `digital.ico` usage across the executable, window title, and taskbar icons.

### Known Issues / Optional Enhancements
- Heatmap controls remain disabled unless a future `tkintermapview` release exposes `set_heatmap`/`delete_all_markers`; revisit when upstream support lands.

## [2.1.5] - 2025-09-03
- Integrated forensic tool support across forms, graphs, and reports.
- Enhanced reporting with benchmarking tables, aging alerts, and dynamic formatting.
- Expanded map analytics with marker metrics, geocoding caching, and heatmap scaffolding.
