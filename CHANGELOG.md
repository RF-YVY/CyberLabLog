# Changelog

All notable changes to this project will be documented in this file.

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
