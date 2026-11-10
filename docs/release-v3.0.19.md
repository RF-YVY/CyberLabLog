# CyberLab Case Tracker v3.0.19

Fixes clipped label edges and missing exhibit numbers on unsuffixed cases.

- Direct printing fits the complete label image inside the Windows driver's reported printable area, with a small 0.02-inch safety allowance on each edge. The image is centered and its proportions are preserved, protecting the right-side logo from clipping without reintroducing browser scaling.
- EXHIBIT defaults to the numeric evidence suffix: CC-26-15529-1 → 1, CC-26-15529-2 → 2, and so on. A base case such as CC-26-15529 defaults to EXHIBIT 1.
- Existing saved labels with an empty exhibit field automatically recover the inferred exhibit number when reopened or printed in a batch. Nonblank manually entered exhibit numbers are retained. Other blank fields remain omitted.
- Dymo feed rotation, direct Windows printing, saved printer choice, custom paper sizes, and PDF/PNG alternatives remain available.

Validation: 64 backend tests and 14 frontend tests pass. Tests exercise the actual Windows drawing-rectangle calculation with nonzero hardware margins, preserved proportions, exhibit suffixes, unsuffixed cases, and older saved labels. The packaged EXE passed printer discovery, direct-print validation without a physical job, saved labels, PDF, completion, county-map export, and report-viewer checks. The installer installed the identical executable and preserved a test database and app_data on uninstall. Physical output still needs confirmation on the office Dymo 550.

Update through Settings → Portable Application Update, close the old app tabs, reopen CyberLab, and print one test label. You do not need to re-enter saved label data to restore missing exhibits.

Downloads: CyberLab-Case-Tracker-v3.0.19.exe (portable), Case-Tracker-Setup-v3.0.19.exe (per-user installer), and matching SHA-256 files.
