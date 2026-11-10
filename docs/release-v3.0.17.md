# CyberLab Case Tracker v3.0.17

Fixes label printing getting stuck on a tab that says "Preparing label PDF" on some office browsers.

- Print Label and Print All Labels now create the PDF inside CyberLab and open it through a normal local HTTP address. Printing no longer depends on browser-side PDF creation or navigation to a browser blob URL.
- Print previews provide ordinary Open label PDF and Download PDF links when automatic navigation is restricted.
- Image preparation, label saving, and PDF requests have timeouts with useful errors instead of waiting indefinitely. The preview tab also displays failures.
- Labels retain their exact configured paper size, one label per page, without headers, footers, or extra page margins. Choose the matching stock and Actual size / 100% in the PDF viewer.
- PNG downloads, saved exhibit fields, and county maps remain available.

Install this update on the office PC, close the older application/browser tabs, then reopen CyberLab and retry Print Label. Office browser settings may download the PDF instead of displaying it; open that downloaded PDF to print.

Downloads: **CyberLab-Case-Tracker-v3.0.17.exe** (portable), **Case-Tracker-Setup-v3.0.17.exe** (per-user installer), and matching SHA-256 files. Existing users can use Settings → Portable Application Update → Check → Download & Install.

Validation: 59 backend tests and 12 frontend tests pass, including ordinary HTTP PDF responses, exact multi-page label dimensions, blocked automatic navigation with fallback links, and stalled-operation timeouts. A browser print test opened the normal HTTP PDF; its rendered layout was visually checked. The packaged Windows EXE passed the new HTTP PDF check and the saved-label, completion, county-map and report-viewer checks. The installer installed an identical EXE, and uninstall preserved a test database and app_data. The specific office PC still needs a retry after updating.
