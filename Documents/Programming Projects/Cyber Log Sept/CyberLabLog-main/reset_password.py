# reset_password.py
"""
Reset the application password to 'password' for CyberLabCaseTracker.
Run this script with the same Python environment as your main app.
"""
import sqlite3
import secrets
import hashlib
import os

DB_FILENAME = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'caselog_gui_v6.db')

DEFAULT_PASSWORD = "password"

def generate_salt(length=16):
    return secrets.token_hex(length)

def hash_password(password, salt):
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), 100000)
    return hashed.hex()

def reset_password():
    salt = generate_salt()
    hashed_password = hash_password(DEFAULT_PASSWORD, salt)
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", ('password_hash', hashed_password))
    cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", ('salt', salt))
    conn.commit()
    conn.close()
    print("Password has been reset to 'password'.")

if __name__ == "__main__":
    reset_password()
