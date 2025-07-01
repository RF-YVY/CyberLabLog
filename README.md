# CyberLab Case Tracker

A digital forensics case log and reporting tool for labs and agencies, built with Python, Tkinter, and ttkbootstrap. CyberLab Case Tracker provides robust case management, advanced reporting, mapping, and analytics for digital forensic investigations.

## Features

- **New Case Entry:** Add new digital forensic cases with examiner, agency, offense, device, and more. Auto-populates last used examiner and state.
- **View Data:** Browse, search, filter, edit, and delete case records. Undo/redo support for edits. Customizable columns.
- **Map View:** Visualize case locations by city/state on an interactive map. Select a focal state to center the map.
- **Graphs:** Generate charts by offense type, device, agency, examiner, and more. Graphs resize responsively to the window.
- **Reports:** Export PDF/XLSX reports (full, summary, custom, or selected rows). Persistent report header info (agency, division, name, date) included in all reports.
- **Import/Export:** Import cases from Excel, export to PDF/XLSX.
- **Accessibility:** Keyboard navigation, context menus, and tooltips.
- **Customizable:** Change themes, logo, and map marker icon.
- **Security:** Password-protected data deletion and settings.
- **About Tab:** Comprehensive app info and clickable developer GitHub link.

## Data Storage

- All case data is stored locally in an encrypted SQLite database (`caselog_gui_v6.db`).
- User preferences and settings are stored in the `app_data` directory.
- No data is sent to the cloud or external servers.

## Requirements

- Python 3.8+
- [ttkbootstrap](https://ttkbootstrap.readthedocs.io/)
- [tkintermapview](https://github.com/TomSchimansky/TkinterMapView)
- [matplotlib](https://matplotlib.org/)
- [openpyxl](https://openpyxl.readthedocs.io/)
- [reportlab](https://www.reportlab.com/)
- [geopy](https://geopy.readthedocs.io/)

Install dependencies with:

```
pip install ttkbootstrap tkintermapview matplotlib openpyxl reportlab geopy
```

## Usage

1. Run `CyberLabCaseTracker.py` with Python 3.8+:
   ```
   python CyberLabCaseTracker.py
   ```
2. Use the tabs to add, view, map, and analyze cases.
3. Access settings to customize the app, import data, or change the theme.
4. Use the About tab for version info and support.

## GitHub Safety & .gitignore

- **Do NOT commit user data, database files, logs, or sensitive info.**
- The provided `.gitignore` excludes all user data, database, logs, and cache files.
- Only source code, documentation, and static assets should be pushed to GitHub.

## .gitignore Example

```
# User data and database
caselog_gui_v6.db
app_data/
*.log
*.sqlite*
*.db*
__pycache__/
*.pyc
*.pyo
*.pyd
.DS_Store
.env
*.xlsx
*.pdf
logo.png
marker_icon.png
```

## Support & Documentation

- For help, documentation, or updates, contact your system administrator or the application provider.
- This tool is designed for internal use by digital forensics labs and law enforcement agencies.
- Developer: RF-YVY ([GitHub](https://github.com/RF-YVY))

## License

This project is intended for internal, non-commercial use. See LICENSE file if provided.

<a href="https://www.flaticon.com/free-icons/forensics" title="forensics icons">Forensics icons created by Iconjam - Flaticon</a>
