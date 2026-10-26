# CyberLab Case Tracker v3.0.13

Fixes the **Access Blocked / 403** background in exported map reports.

- Exported HTML now embeds Leaflet and a public-domain geographic overview with country and U.S. state boundaries. It works without internet, retaining markers, case totals, popups, zoom controls, and the location table.
- Local HTML files no longer request OpenStreetMap street tiles, because browsers do not send the HTTP referrer required by OSM's tile policy from `file://`.
- For street detail, use **Reports → output files → Open Map Report** while CyberLab is running. This HTTP viewer supports older exported map snapshots, uses the canonical tile URL with normal browser referrers and attribution, and falls back to the geographic overview if tiles fail.
- The built-in overview provides regional context, not road-level detail or survey boundaries. The in-app map continues to work as before.

**Existing exports:** regenerate the map HTML for standalone support, or open an earlier report through **Open Map Report**. Updating the app does not rewrite previously exported files.

**Update:** v3.0.12 users can choose **Settings → Portable Application Update → Check → Download & Install**. Earlier versions require manually replacing the EXE once, preserving the existing database and `app_data` folder.

Validation: 36 backend tests and 3 frontend tests, production build, and map-report viewer checked with synthetic data. A JavaScript runtime regression verifies that local files request no tiles and that HTTP tile failures restore the geographic overview. The Windows EXE generated a map with embedded assets and served it through the report viewer; the real executable replacement/restart handoff also passed.
