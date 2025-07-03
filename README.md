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

![Image](https://github.com/user-attachments/assets/a2e67522-42d9-4975-9d7a-85f0b579b4ec)

![Image](https://github.com/user-attachments/assets/928c3bde-f2b6-4aff-8ec2-977e5679d290)

![Image](https://github.com/user-attachments/assets/538a85f3-8eef-4fa0-a420-418cf6b599f4)

![Image](https://github.com/user-attachments/assets/b9b63681-cca2-44e3-b646-079ab1104ca5)

![Image](https://github.com/user-attachments/assets/aead7596-9ebc-4ecf-9814-4b515b32b131)

![Image](https://github.com/user-attachments/assets/6c4fe01a-a5cc-4e17-afb9-55cedafef894)

## XLSX Import Format

When importing cases from an Excel file, the following column headers are **required** (case-sensitive):

| Column Header | Description | Format/Type |
|---------------|-------------|-------------|
| `ID` | Unique identifier | Optional, can be empty |
| `Case #` | Case number or identifier | Text |
| `Examiner` | Name of the examiner | Text |
| `Investigator` | Name of the investigator | Text |
| `Agency` | Agency or organization name | Text |
| `City` | City where offense occurred | Text |
| `State` | State where offense occurred | Text |
| `Start (MM-DD-YYYY)` | Case start date | MM-DD-YYYY format |
| `End (MM-DD-YYYY)` | Case end date | MM-DD-YYYY format |
| `Vol (GB)` | Volume size in gigabytes | Numeric |
| `Offense` | Type of offense or crime | Text |
| `Device` | Type of device examined | Text |
| `Model` | Device model | Text |
| `OS` | Operating system | Text |
| `Recovered?` | Data recovery status | Yes/No |
| `FPR?` | Full Physical Recovery status | Yes/No |
| `Notes` | Additional notes or comments | Text |
| `Created (YYYY-MM-DD)` | Creation date | YYYY-MM-DD format |

**Important Notes:**
- All column headers must match exactly (case-sensitive)
- Missing any required column will cause the import to fail
- Date formats must be exactly as specified
- Boolean fields (Recovered?, FPR?) should contain "Yes" or "No"

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

# Download exe from Releases (no setup) or...

## Install dependencies with:

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
