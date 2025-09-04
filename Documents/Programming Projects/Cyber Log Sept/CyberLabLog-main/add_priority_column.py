import sqlite3

# Path to your SQLite database file
DB_FILENAME = r'C:\Users\NCFI Student\Documents\Programming Projects\Cyber Log Sept\CyberLabLog-main\caselog_gui_v6.db'

conn = sqlite3.connect(DB_FILENAME)
cursor = conn.cursor()

for col, ddl in [
    ("priority", "ALTER TABLE case_log ADD COLUMN priority TEXT DEFAULT 'Medium'"),
    ("workflow_status", "ALTER TABLE case_log ADD COLUMN workflow_status TEXT DEFAULT 'Intake'"),
    ("target_due_date", "ALTER TABLE case_log ADD COLUMN target_due_date TEXT")
]:
    try:
        cursor.execute(ddl)
        print(f"Column '{col}' added to case_log.")
    except sqlite3.OperationalError as e:
        if 'duplicate column name' in str(e).lower():
            print(f"Column '{col}' already exists in case_log.")
        else:
            print(f"Error adding column '{col}': {e}")
conn.close()
