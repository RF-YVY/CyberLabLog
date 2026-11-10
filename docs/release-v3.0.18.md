# CyberLab Case Tracker v3.0.18

Direct Windows label printing, using the feed-direction approach verified in the supplied Evidence Label Tool.

- Print Label now sends the label image directly to the selected Windows printer, bypassing the browser/PDF print dialog and its scaling.
- For Dymo printers, landscape labels are rotated 90 degrees counter-clockwise to match the printer's feed direction. A 4 × 2 5/16-inch landscape label is sent as 2 5/16 inches across the print head and 4 inches along the feed direction.
- The print job selects matching paper dimensions from the driver, preferring the 30256 stock when available, sets portrait feed orientation and zero page margins, and draws the image across the full physical label. Driver-size mismatches produce an error instead of silently printing a miniature label.
- A Label printer selector is available in label settings, the label generator, and case-wide output. Dymo 550 is preferred when detected. Save Label Settings to retain the selected default printer.
- Print All Labels uses the same direct path for all exhibits in the case. Open Print PDF / Open All Labels PDF remain available as explicit alternatives.
- Label-image preparation skips unrelated web-font embedding. Direct printing uses one Windows print process per batch and avoids the PDF preparation/viewer steps.
- Saved exhibit labels, PNG downloads, custom sizes, QR codes, and report maps remain available.

After updating, open Settings → Evidence Label Settings, select the Dymo 550 under Label printer, and save. Open an exhibit and click Print Label. That button sends directly to the selected printer; the separate PDF button opens a PDF preview.

Validation: 63 backend tests and 13 frontend tests pass. Tests verify Dymo geometry, counter-clockwise image rotation, multi-label jobs, printer selection, and direct-print request data. Single and bulk UI print actions completed against an intercepted test queue without physical jobs. Windows printer discovery and PrintDocument paper configuration were checked locally; the print script passed PowerShell syntax validation. The packaged EXE passed printer discovery, missing-printer validation, saved-label, HTTP PDF, completion, county-map and report-viewer checks. The installer installed an identical executable and preserved test data when uninstalled. A Dymo 550 is not connected to the build machine, so actual paper size and output must be confirmed on the office printer.

Downloads: CyberLab-Case-Tracker-v3.0.18.exe (portable), Case-Tracker-Setup-v3.0.18.exe (per-user installer), and their SHA-256 checksums.
