# Report Parity Review

Use this checklist to compare CyberLab Case Tracker modern exports against representative legacy exports.

## Test Data

- Use a database with completed cases, in-progress cases, multiple agencies, multiple device types, and at least one case with a large volume total.
- Use a configured report logo.
- Use a configured custom map marker.
- Run exports with date scope set to `all` first, then test `current_week` and `current_month`.

## Expected Modern Export Files

- Completed cases CSV
- In-progress cases CSV
- Summary JSON
- Summary PDF
- Summary XLSX
- Graph CSV files
- Graph PNG files
- Map HTML
- Map data JSON

## Legacy Comparison Points

- Case count matches completed case total.
- In-progress count matches active case total.
- Total volume matches legacy output, including GB to TB conversion when applicable.
- Agency, examiner, investigator, offense type, device type, city, and state groupings match.
- Report logo appears and is readable.
- Profile fields show Agency/Organization and Name where expected.
- Graph labels and values match the legacy graph families.
- Map output includes the same geocoded locations.
- Case detail fields are present in tabular output.
- PDF page size and orientation match selected settings.
- XLSX workbook has usable column widths, readable headers, and complete rows.

## Current Known Differences To Review

- Modern exports use the native browser migration engine rather than the legacy GUI export bridge.
- Scheduled exports run only while the modern app is open.
- Dropdown delete/rename affects future dropdown choices, not historical case records.
- Map marker transparency is preserved in modern browser map output.

## Sign-Off Notes

- Sample database:
- Legacy export folder:
- Modern export folder:
- Reviewer:
- Date:
- Result:
