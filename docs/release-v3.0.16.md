# CyberLab Case Tracker v3.0.16

Evidence labels and calmer report maps, with portable and installer downloads.

- Generate evidence labels from New Case, completed cases, and in-progress case details. Agency/division titles and a right-side logo persist in label settings.
- EXHIBIT defaults to the final numeric case suffix. Case data fills editable label fields; blank fields are omitted, and additional fields can be added.
- Save label fields, extra fields, and QR values per exhibit. New Case keeps them in its draft until the case is saved. Completing an in-progress item retains its label; the next item and duplicates start fresh.
- Print one label or all exhibits in a case through exact-size PDFs with no headers, footers, or page margins. Choose matching printer stock and Actual size / 100% in the PDF viewer.
- Default stock is Dymo 550 shipping labels, 4 inches wide × 2 5/16 inches high. Custom dimensions remain available. Labels stay within the selected size; excessive text must be shortened before output.
- Download one PNG or a ZIP containing a separate PNG for every completed/in-progress exhibit in the case.
- Report maps default to a soft-colored custom county map with thin county boundaries, county names, selected city names, and labels that avoid markers and each other. Generation and viewing work offline without an API key.
- Optionally include an embedded USGS detail layer from Reports → Output Options. USGS imagery is fetched during generation, cached locally, and available through Layers. The calmer county map stays selected initially. The in-app map is unchanged.
- Adds a per-user Windows installer alongside the portable executable. No administrator privileges are required. Application-created databases and app_data are retained when uninstalling. The in-app updater selects the portable executable, not the installer.

## Downloads

- **CyberLab-Case-Tracker-v3.0.16.exe:** portable application. Replace the old EXE in your current application folder; keep the existing database and app_data beside it.
- **Case-Tracker-Setup-v3.0.16.exe:** optional per-user installer. Its default location is `%LOCALAPPDATA%\Programs\CyberLab Case Tracker`. Select your existing writable application folder to use existing data, or import your previous database/assets from Settings after installation.
- Matching `.sha256` files verify each download.

Existing users can update through **Settings → Portable Application Update → Check → Download & Install**. Regenerate older maps to use the new county style. No user databases, uploaded branding, or report outputs are included in the release downloads.

Validation: 56 backend tests and 11 frontend tests pass. Browser checks verified per-exhibit label persistence, fresh labels for the next exhibit, multi-exhibit PNG downloads and PDF preparation, and the custom county map. The packaged Windows EXE passed isolated startup, saved-label, completion, offline county-map export, and report-viewer checks. The per-user installer installed an identical executable, and uninstall preserved a test case database and app_data. The packaged archive includes county geometry and no user databases. SHA-256 checksums accompany both downloads.

The installer download is named without the CyberLab prefix so versions with the older portable updater cannot mistake it for the application EXE. The build produces CyberLab-Case-Tracker-Setup-v3.0.16.exe; release packaging renames that file without changing its contents.
