import sqlite3
import hashlib
import os
import base64

DB_PATH = os.path.join(os.path.dirname(__file__), 'caselog_gui_v6.db')
DEFAULT_PASSWORD = 'password'

def get_salt(cursor):
    cursor.execute("SELECT value FROM settings WHERE key = 'salt'")
    row = cursor.fetchone()
    return row[0] if row else None

def get_password_hash(cursor):
    cursor.execute("SELECT value FROM settings WHERE key = 'password_hash'")
    row = cursor.fetchone()
    return row[0] if row else None

def hash_password(password, salt):
    return base64.b64encode(hashlib.pbkdf2_hmac(
        'sha256', password.encode('utf-8'), salt.encode('utf-8'), 100000
    )).decode('utf-8')

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    salt = get_salt(cursor)
    if not salt:
        print('No salt found in settings table.')
        return
    current_hash = get_password_hash(cursor)
    default_admin_hash = hash_password('admin', salt)
    if current_hash == default_admin_hash:
        new_hash = hash_password(DEFAULT_PASSWORD, salt)
        cursor.execute("UPDATE settings SET value = ? WHERE key = 'password_hash'", (new_hash,))
        conn.commit()
        print('Default password updated to "password".')
    else:
        print('Password has already been changed by the user. No update performed.')
    conn.close()

if __name__ == '__main__':
    main()
