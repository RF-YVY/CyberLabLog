import sqlite3
import importlib.util
from pathlib import Path

# Load the app module from the local file
repo_root = Path(r"c:\Users\NCFI Student\Documents\Programming Projects\CyberLabLog-main")
app_path = repo_root / "CyberLabCaseTracker.py"
spec = importlib.util.spec_from_file_location("appmod", str(app_path))
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)

# Use a temporary DB to avoid clobbering real data
app.DB_FILENAME = str(repo_root / 'caselog_gui_v6_smoketest.db')

# Init DB
app.init_db()

# 1) Add a completed case
case = {
    'case_number': 'T-123',
    'examiner': 'X',
    'investigator': 'I',
    'agency': 'A',
    'city_of_offense': 'C',
    'state_of_offense': 'NC',
    'start_date': '2024-01-01',
    'end_date': '2024-01-02',
    'volume_size_gb': 10,
    'offense_type': 'O',
    'device_type': 'Phone',
    'model': 'M',
    'os': 'Android',
    'data_recovered': 'Yes',
    'fpr_complete': 1,
    'notes': 'n',
}
assert app.add_case_db(case)

# Find the inserted id
conn = sqlite3.connect(app.DB_FILENAME)
cur = conn.cursor()
cur.execute("SELECT id FROM case_log WHERE case_number=?", ('T-123',))
row = cur.fetchone()
assert row, "Inserted case not found"
cid = row[0]
conn.close()

# 2) Update
assert app.update_case_db(cid, {'notes': 'updated'})

# 3) Fetch recent activities
acts = app.get_recent_activities(limit=10)
print('RECENT:', [ (a['activity_type'], a.get('case_number')) for a in acts ])

# 4) Delete and fetch again
assert app.delete_case_db(cid)
acts2 = app.get_recent_activities(limit=10)
print('AFTER_DEL:', [ (a['activity_type'], a.get('case_number')) for a in acts2 ])

print('SMOKETEST_OK')
