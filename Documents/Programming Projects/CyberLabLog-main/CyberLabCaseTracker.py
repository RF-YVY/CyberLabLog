
# --- Move Report Header Info Persistence into CaseLogApp ---


# --- Main Application Class ---

# (Moved report header info methods to the main CaseLogApp class below)


import ttkbootstrap as tb
from ttkbootstrap.widgets import DateEntry
from ttkbootstrap.constants import *
from ttkbootstrap.dialogs import Messagebox
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog, scrolledtext
import sqlite3
import os
import io
import time
from datetime import datetime, timedelta, date as datetime_date # For isinstance check
from PIL import Image, ImageTk
import shutil
import logging
import hashlib # For password hashing
import secrets # For generating salt
import sys # For exiting gracefully
import random
import threading # For running long tasks in background
import queue # For inter-thread communication
import calendar
import json
import requests
import pandas as pd

# --- ReportLab ---
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as ReportLabImage, PageBreak
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT # For text alignment
from reportlab.pdfbase.pdfmetrics import stringWidth
from copy import copy as copy_style

# --- Mapping & Geocoding ---
import tkintermapview
from geopy.geocoders import Nominatim
import functools
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# --- App Constants & Paths ---
APP_NAME = "CyberLab Case Tracker"
APP_VERSION = "2.1.0"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'app_data')
DB_FILENAME = os.path.join(BASE_DIR, 'caselog_gui_v6.db')
LOG_FILENAME = os.path.join(DATA_DIR, 'app.log')
LOGO_FILENAME = os.path.join(DATA_DIR, 'logo.png')
MARKER_ICON_FILENAME = os.path.join(DATA_DIR, 'marker_icon.png')
BACKUP_DIR = os.path.join(DATA_DIR, 'backups')
DEFAULT_PASSWORD = "password"
RELEASES_URL = "https://github.com/RF-YVY/CyberLabLog/releases"
UPDATE_CHECK_URL = "https://api.github.com/repos/RF-YVY/CyberLabLog/releases/latest"
THEME_OPTIONS = [
    ("Flatly", "flatly"),
    ("Darkly", "darkly"),
    ("Cyborg", "cyborg"),
    ("Cosmo", "cosmo"),
    ("Journal", "journal"),
    ("Minty", "minty"),
    ("Pulse", "pulse"),
    ("Sandstone", "sandstone"),
    ("Simplex", "simplex"),
    ("Solar", "solar"),
    ("Superhero", "superhero"),
    ("United", "united"),
    ("Yeti", "yeti")
]

# Common US state abbreviations (used in combos)
US_STATE_ABBREVIATIONS = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS",
    "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY",
    "NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV",
    "WI","WY","DC"
]


def init_db():
    """Initialize application data directory, logging, and SQLite tables."""
    try:
        # Ensure data directory exists
        os.makedirs(DATA_DIR, exist_ok=True)
        # Ensure backup directory exists
        os.makedirs(BACKUP_DIR, exist_ok=True)

        # Configure logging once
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s [%(levelname)s] %(message)s',
                handlers=[
                    logging.FileHandler(LOG_FILENAME, encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )

        # Initialize database and tables
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()

        # case_log table
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS case_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_number TEXT,
                examiner TEXT,
                offense_type TEXT,
                device_type TEXT,
                start_date TEXT,
                end_date TEXT,
                volume_size_gb REAL,
                city_of_offense TEXT,
                state_of_offense TEXT,
                investigator TEXT,
                agency TEXT,
                model TEXT,
                os TEXT,
                data_recovered TEXT,
                fpr_complete INTEGER,
                notes TEXT,
                created_at TEXT
            )'''
        )

        # geocache table
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS geocache (
                location_key TEXT PRIMARY KEY,
                latitude REAL,
                longitude REAL,
                last_accessed TEXT
            )'''
        )

        # settings table (key/value)
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )'''
        )

        # Seed default password on first run if none exists
        try:
            cursor.execute("SELECT value FROM settings WHERE key = 'password_hash'")
            row = cursor.fetchone()
            if not row:
                salt = generate_salt()
                hashed = hash_password(DEFAULT_PASSWORD, salt)
                cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", ('password_hash', hashed))
                cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", ('salt', salt))
                logging.info("Seeded default password in settings (first run)")
        except Exception as e:
            logging.warning(f"Could not seed default password: {e}")

        conn.commit()
    except Exception as e:
        # Log but don't raise to avoid startup crash; caller will handle errors
        print(f"init_db error: {e}")
        logging.error(f"init_db error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

# --- Database Backup Utilities ---
def perform_db_backup(retention_days: int = 56):
    """Create a timestamped backup of the SQLite DB and prune old backups.
    retention_days: number of days to keep backups (default ~8 weeks).
    """
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(DB_FILENAME))[0]
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_path = os.path.join(BACKUP_DIR, f"{base_name}_{ts}.db")

        # Try safe sqlite backup; fallback to file copy
        try:
            src = sqlite3.connect(DB_FILENAME)
            dest = sqlite3.connect(backup_path)
            with dest:
                src.backup(dest)
            src.close()
            dest.close()
        except Exception as e:
            logging.warning(f"SQLite backup() failed ({e}); falling back to copy2.")
            shutil.copy2(DB_FILENAME, backup_path)

        logging.info(f"Database backup created: {backup_path}")

        # Prune backups older than retention_days
        cutoff = datetime.now() - timedelta(days=retention_days)
        try:
            for fname in os.listdir(BACKUP_DIR):
                if not fname.startswith(base_name + "_"):
                    continue
                if not fname.lower().endswith('.db'):
                    continue
                fpath = os.path.join(BACKUP_DIR, fname)
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
                    if mtime < cutoff:
                        os.remove(fpath)
                        logging.info(f"Pruned old backup: {fpath}")
                except Exception as e:
                    logging.debug(f"Skip pruning {fpath}: {e}")
        except Exception as e:
            logging.debug(f"Backup pruning skipped: {e}")
    except Exception as e:
        logging.error(f"Database backup failed: {e}")

def get_cached_location_db(location_key):
    """Retrieves cached latitude and longitude for a location_key."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("SELECT latitude, longitude FROM geocache WHERE location_key = ?", (location_key,))
        row = cursor.fetchone()
        if row:
            # Optionally, update last_accessed timestamp if you want to manage cache eviction later
            # timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # cursor.execute("UPDATE geocache SET last_accessed = ? WHERE location_key = ?", (timestamp, location_key))
            # conn.commit()
            logging.debug(f"Cache hit for location_key: {location_key}")
            return row[0], row[1]
        logging.debug(f"Cache miss for location_key: {location_key}")
        return None
    except Exception as e:
        logging.error(f"Error retrieving cached location for '{location_key}': {e}")
        return None
    finally:
        if conn:
            conn.close()

def add_cached_location_db(location_key, latitude, longitude):
    """Adds or updates a location in the geocache."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
            INSERT OR REPLACE INTO geocache (location_key, latitude, longitude, last_accessed)
            VALUES (?, ?, ?, ?)
        ''', (location_key, latitude, longitude, timestamp))
        conn.commit()
        logging.info(f"Cached/Updated location '{location_key}': {latitude}, {longitude}")
        return True
    except Exception as e:
        logging.error(f"Error caching location '{location_key}': {e}")
        return False
    finally:
        if conn:
            conn.close()

def add_case_db(case_data):
    """Adds a new case to the database."""
    conn = None
    try:
        db_path = os.path.abspath(DB_FILENAME)
        logging.info(f"[add_case_db] Using database file: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Convert boolean for fpr_complete to integer 0 or 1
        fpr_int = 1 if case_data.get("fpr_complete") else 0
        # Convert data_recovered to standardized string format
        dr_val = case_data.get("data_recovered")
        if dr_val is True or (isinstance(dr_val, str) and dr_val.lower() in ['yes', 'true', '1']):
            dr_str = "Yes"
        elif dr_val is False or (isinstance(dr_val, str) and dr_val.lower() in ['no', 'false', '0']):
            dr_str = "No"
        else:
            dr_str = ""

        # Get current timestamp for created_at
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute('''
            INSERT INTO case_log (
                case_number, examiner, investigator, agency, city_of_offense, state_of_offense,
                start_date, end_date, volume_size_gb, offense_type, device_type, model, os,
                data_recovered, fpr_complete, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            str(case_data.get("case_number")).strip() if case_data.get("case_number") is not None else None,
            case_data.get("examiner"),
            case_data.get("investigator"),
            case_data.get("agency"),
            case_data.get("city_of_offense"),
            case_data.get("state_of_offense"),
            case_data.get("start_date"),
            case_data.get("end_date"),
            case_data.get("volume_size_gb"),
            case_data.get("offense_type"),
            case_data.get("device_type"),
            case_data.get("model"),
            case_data.get("os"),
            dr_str,
            fpr_int,
            case_data.get("notes"),
            created_at
        ))
        conn.commit()
        logging.info(f"Case '{case_data.get('case_number', '')}' added to database.")
        return True
    except Exception as e:
        logging.error(f"Error adding case '{case_data.get('case_number', 'N/A')}' to database: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_all_cases_db():
    """Retrieves all cases from the database."""
    conn = None
    try:
        db_path = os.path.abspath(DB_FILENAME)
        logging.info(f"[get_all_cases_db] Using database file: {db_path}")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # To access columns by name
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM case_log")
        rows = cursor.fetchall()
        # Convert rows to list of dictionaries
        return [dict(row) for row in rows]
    except Exception as e:
        logging.error(f"Error retrieving all cases from database: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_case_by_number_db(case_number):
    """Retrieves a single case by its case number."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        conn.row_factory = sqlite3.Row  # To access columns by name
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM case_log WHERE case_number = ?", (str(case_number).strip(),)) # Ensure search is stripped
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logging.error(f"Error retrieving case by number '{case_number}': {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_case_by_id_db(case_id):
    """Retrieves a single case by its database ID."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        conn.row_factory = sqlite3.Row  # To access columns by name
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM case_log WHERE id = ?", (case_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logging.error(f"Error retrieving case by ID '{case_id}': {e}")
        return None
    finally:
        if conn:
            conn.close()


def update_case_db(case_id, case_data):
    """Updates an existing case record in the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()

        # Construct the SET part of the SQL query dynamically
        # Exclude 'id' and 'created_at' but allow 'case_number' to be updated
        fields_to_update = [field for field in case_data.keys() if field not in ['id', 'created_at']]
        set_clause = ', '.join([f'{field} = ?' for field in fields_to_update])

        if not set_clause:
            logging.warning(f"No valid fields to update for case ID {case_id}.")
            return False # Nothing to update

        # Convert boolean for fpr_complete to integer 0 or 1 for database
        if 'fpr_complete' in fields_to_update:
             case_data['fpr_complete'] = 1 if case_data.get('fpr_complete') else 0

        # Convert data_recovered to standardized string format
        if 'data_recovered' in fields_to_update:
            dr_val = case_data.get('data_recovered')
            if dr_val is True or (isinstance(dr_val, str) and dr_val.lower() in ['yes', 'true', '1']):
                case_data['data_recovered'] = "Yes"
            elif dr_val is False or (isinstance(dr_val, str) and dr_val.lower() in ['no', 'false', '0']):
                case_data['data_recovered'] = "No"
            else:
                case_data['data_recovered'] = ""


        # Prepare the values tuple, ensuring the order matches the set_clause
        values = tuple(case_data[field] for field in fields_to_update) + (case_id,)

        cursor.execute(f'''
            UPDATE case_log
            SET {set_clause}
            WHERE id = ?
        ''', values)
        conn.commit()
        logging.info(f"Case ID {case_id} updated successfully in DB.")
        return True
    except Exception as e:
        logging.error(f"Failed to update case ID {case_id} in DB: {e}")
        # show_error ("DB Error", f"Update case failed for ID {case_id}: {e}"); # Avoid messagebox in helper
        return False
    finally:
        if conn:
            conn.close()


def delete_case_db(case_id):
    """Deletes a case record from the database by its ID."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM case_log WHERE id = ?", (case_id,))
        conn.commit()
        logging.info(f"Case ID {case_id} deleted successfully from DB.")
        return True
    except Exception as e:
        logging.error(f"Failed to delete case ID {case_id} from DB: {e}")
        # show_error ("DB Error", f"Delete case failed for ID {case_id}: {e}"); # Avoid messagebox in helper
        return False
    finally:
        if conn:
            conn.close()


def generate_salt(length=16):
    """Generates a random salt for password hashing."""
    return secrets.token_hex(length)

def hash_password(password, salt):
    """Hashes a password using PBKDF2."""
    # Use a strong KDF like PBKDF2
    # It's recommended to use a higher number of iterations in production
    hashed = hashlib.pbkdf2_hmac('sha256',
                                 password.encode('utf-8'), # Convert password to bytes
                                 salt.encode('utf-8'),     # Convert salt to bytes
                                 100000) # Number of iterations
    return hashed.hex() # Convert hash to hex string for storage

def verify_password(password):
    """Verifies a password against the stored hash and salt."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = 'password_hash'")
        stored_hash_row = cursor.fetchone()
        cursor.execute("SELECT value FROM settings WHERE key = 'salt'")
        stored_salt_row = cursor.fetchone()

        if stored_hash_row and stored_salt_row:
            stored_hash = stored_hash_row[0]
            stored_salt = stored_salt_row[0]
            # Hash the provided password with the stored salt
            hashed_provided_password = hash_password(password, stored_salt)
            return hashed_provided_password == stored_hash
        else:
            logging.warning("Password hash or salt not found in settings DB.")
            return False # Should not happen if init_db runs correctly
    except Exception as e:
        logging.error(f"Error verifying password: {e}")
        return False
    finally:
        if conn:
            conn.close()

def update_password_db(new_password):
    """Updates the stored password hash and salt in the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        salt = generate_salt()
        hashed_password = hash_password(new_password, salt)
        cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", ('password_hash', hashed_password))
        cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", ('salt', salt))
        conn.commit()
        logging.info("Password updated successfully in DB.")
        return True
    except Exception as e:
        logging.error(f"Error updating password in DB: {e}")
        return False
    finally:
        if conn:
            conn.close()

def has_password_set():
    """Return True if a password hash and salt are present in settings."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM settings WHERE key = 'password_hash'")
        has_hash = cursor.fetchone() is not None
        cursor.execute("SELECT 1 FROM settings WHERE key = 'salt'")
        has_salt = cursor.fetchone() is not None
        return has_hash and has_salt
    except Exception as e:
        logging.debug(f"has_password_set check failed: {e}")
        return False
    finally:
        if conn:
            conn.close()


def get_combo_values_db(key):
    """Retrieve a list of combo values for a given key from the settings table."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = ?", (f"combo_{key}",))
        row = cursor.fetchone()
        if row and row[0]:
            # Use JSON for robust storage
            return json.loads(row[0])
        return []
    except Exception as e:
        logging.error(f"Error retrieving combo values for '{key}': {e}")
        return []
    finally:
        if conn:
            conn.close()

def set_combo_values_db(key, values):
    """Store a list of combo values for a given key in the settings table."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        value_str = json.dumps(values)
        cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (f"combo_{key}", value_str))
        conn.commit()
    except Exception as e:
        logging.error(f"Error saving combo values for '{key}': {e}")
    finally:
        if conn:
            conn.close()

def set_user_pref(key, value):
    set_combo_values_db(f"userpref_{key}", [value])

def get_user_pref(key, default=None):
    vals = get_combo_values_db(f"userpref_{key}")
    return vals[0] if vals else default

# --- Helper Functions ---

def format_date_str_for_display(date_str):
    """Formats aYYYY-MM-DD date string to MM-DD-YYYY for display."""
    if not date_str:
        return ""
    try:
        # Attempt to parse bothYYYY-MM-DD andYYYY-MM-DD HH:MM:SS formats
        try:
            date_obj = datetime.strptime(str(date_str), '%Y-%m-%d').date()
        except ValueError: # Try with time if initial parse fails
             date_obj = datetime.strptime(str(date_str), '%Y-%m-%d %H:%M:%S').date()

        return date_obj.strftime('%m-%d-%Y')
    except Exception:
        logging.warning(f"Could not parse date string '{date_str}' for display formatting.")
        return str(date_str) # Return original if parsing fails


def format_bool_int(value):
    """Formats a 0 or 1 integer to 'Yes', 'No', or '' for display."""
    if value == 1:
        return "Yes"
    elif value == 0:
        return "No"
    else:
        return "" # Handle None or other values


def get_unique_field_values(field):
    """Return a list of unique values for a given field from all cases."""
    cases = get_all_cases_db()
    values = set()
    for case in cases:
        val = (case.get(field) or "").strip()
        if val:
            values.add(val)
    return sorted(values)


# --- Main Application Class ---


class CaseLogApp:
    # --- UI Helpers ---
    def _show_message(self, kind: str, title: str, message: str, max_title: int = 24):
        """Unified messagebox helper that keeps window titles short.
        - If the title exceeds max_title chars, move it into the message body and use a short generic title.
        kind: 'info' | 'error' | 'warning'
        """
        try:
            short_title = (title or "").strip()
            body = (message or "").strip()
            if len(short_title) > max_title:
                # Push the full title into the body for readability and keep a compact window title
                generic = {
                    'info': 'Notice',
                    'error': 'Error',
                    'warning': 'Warning',
                }.get(kind, 'Notice')
                body = f"{short_title}\n\n{body}" if body else short_title
                short_title = generic
            if kind == 'error':
                Messagebox.show_error(short_title, body)
            elif kind == 'warning':
                Messagebox.show_warning(short_title, body)
            else:
                Messagebox.show_info(short_title, body)
        except Exception:
            # Fallback to Tk default messagebox on any edge issues
            if kind == 'error':
                messagebox.showerror("Error", body or short_title)
            elif kind == 'warning':
                messagebox.showwarning("Warning", body or short_title)
            else:
                messagebox.showinfo("Notice", body or short_title)
    def _show_report_saved_dialog(self, filename: str, tone: str = "random"):
        """Show a confirmation dialog that a report was saved, with rotating personalities.
        tone: 'professional' | 'friendly' | 'snarky' | 'fun' | 'random'
        """
        messages = {
            "professional": [
                "Report saved successfully.",
                "Export complete.",
                "The report has been generated and saved.",
            ],
            "friendly": [
                "All set! Your report is tucked away nicely.",
                "Done and dusted. Report saved!",
                "Saved! You’re good to go.",
            ],
            "snarky": [
                "Boom. Report saved. Try not to lose it.",
                "Saved. Told you it’d work.",
                "Report secured. Was that so hard?",
            ],
            "fun": [
                "Ka-ching! Your report just landed.",
                "Mission accomplished. Report extracted!",
                "Report saved. High five!",
            ],
        }
        if tone == "random":
            bucket = random.choice(list(messages.keys()))
        else:
            bucket = tone if tone in messages else "professional"
        title = {
            "professional": "Report Saved",
            "friendly": "Nice!",
            "snarky": "Saved (Obviously)",
            "fun": "Success!",
        }[bucket]
        body_intro = random.choice(messages[bucket])
        Messagebox.show_info(title, f"{body_intro}\n\nSaved to:\n{filename}")
    # --- Update Check Utilities ---
    def _parse_version_tuple(self, v: str):
        try:
            parts = str(v).split(".")
            nums = []
            for p in parts:
                try:
                    nums.append(int(p))
                except Exception:
                    nums.append(0)
            # normalize to 3 components
            while len(nums) < 3:
                nums.append(0)
            return tuple(nums[:3])
        except Exception:
            return (0, 0, 0)

    def check_for_updates(self, silent: bool = False):
        """Notify-only update check against GitHub Releases. No auto-download/install."""
        try:
            # Use a short timeout to avoid blocking UI
            resp = requests.get(UPDATE_CHECK_URL, timeout=6)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            data = resp.json()
            tag = (data.get("tag_name") or "").lstrip("vV")
            html_url = data.get("html_url") or RELEASES_URL
            latest = self._parse_version_tuple(tag)
            current = self._parse_version_tuple(APP_VERSION)
            if latest > current:
                Messagebox.show_info(
                    "Update Available",
                    f"A newer version is available.\n\nCurrent: {APP_VERSION}\nLatest: {tag}\n\nOpen releases page to download?\n{html_url}"
                )
            else:
                if not silent:
                    Messagebox.show_info("Up to date", f"You're on the latest version ({APP_VERSION}).")
        except Exception as e:
            logging.info(f"Update check skipped/failed: {e}")
            if not silent:
                Messagebox.show_info("Update Check", "Could not check for updates right now.")
    def get_map_focal_state(self):
        # Get the persisted focal state, default to empty string (no focus)
        return get_user_pref('map_focal_state', '')

    def set_map_focal_state(self, state):
        set_user_pref('map_focal_state', state)

    def focus_map_on_state(self, state):
        # Center the map on the selected state (if possible)
        if not self.map_widget or not state:
            return
        # Use geopy to get the center of the state
        try:
            geolocator = Nominatim(user_agent=APP_NAME)
            location = geolocator.geocode(f"{state}, USA", timeout=10)
            if location:
                self.map_widget.set_position(location.latitude, location.longitude)
                self.map_widget.set_zoom(6)  # Reasonable zoom for a state
        except Exception as e:
            logging.warning(f"Could not focus map on state '{state}': {e}")
    def get_report_header_info(self):
        info = get_user_pref('report_header_info')
        if info and isinstance(info, dict):
            # Always add current date
            info = info.copy()
            info['Date'] = datetime.now().strftime('%Y-%m-%d')
            return info
        return {"Name": "", "Agency": "", "Division": "", "Date": datetime.now().strftime('%Y-%m-%d')}

    def set_report_header_info(self, info):
        set_user_pref('report_header_info', info)

    def prompt_report_header_info(self):
        info = self.get_report_header_info()
        win = tk.Toplevel(self.root)
        win.title("Report Header Information")
        win.grab_set()
        win.resizable(False, False)
        fields = ["Name", "Agency", "Division"]
        vars = {}
        for i, field in enumerate(fields):
            ttk.Label(win, text=field+":").grid(row=i, column=0, sticky='e', padx=8, pady=6)
            var = tk.StringVar(value=info.get(field, ""))
            entry = ttk.Entry(win, textvariable=var, width=32)
            entry.grid(row=i, column=1, padx=8, pady=6)
            vars[field] = var
        # Date (auto, not editable)
        ttk.Label(win, text="Date:").grid(row=len(fields), column=0, sticky='e', padx=8, pady=6)
        date_str = datetime.now().strftime('%Y-%m-%d')
        date_var = tk.StringVar(value=date_str)
        date_entry = ttk.Entry(win, textvariable=date_var, width=32, state='readonly')
        date_entry.grid(row=len(fields), column=1, padx=8, pady=6)
        def save():
            new_info = {f: vars[f].get().strip() for f in fields}
            new_info['Date'] = date_var.get()
            self.set_report_header_info(new_info)
            win.destroy()
        btn = ttk.Button(win, text="Save", command=save)
        btn.grid(row=len(fields)+1, column=0, columnspan=2, pady=10)
        win.bind('<Return>', lambda e: save())
        win.wait_window()
    # --- Lazy Loading for View Data Treeview ---
    LAZY_PAGE_SIZE = 200  # Number of rows to load per page

    def init_lazy_loading(self):
        self._lazy_offset = 0
        self._lazy_total = 0
        self._lazy_cases = []
        self._lazy_filter = None
        self._lazy_loading = False

    def refresh_data_view(self, filter_text=None, reset_lazy=True):
        """Refresh the Treeview with lazy loading support."""
        # If no filter_text provided, try to use existing filter
        if filter_text is None:
            filter_text = getattr(self, '_view_filter_string', '') if hasattr(self, '_view_filter_string') else ''
        
        if reset_lazy:
            self.init_lazy_loading()
            self._lazy_filter = filter_text
            self._lazy_cases = self.get_filtered_cases(filter_text)
            self._lazy_total = len(self._lazy_cases)
            self._lazy_offset = 0
        self.tree.delete(*self.tree.get_children())
        self.load_next_lazy_page()

    def get_filtered_cases(self, filter_text):
        """Return filtered cases for the current filter/search text."""
        all_cases = get_all_cases_db()
        if not filter_text:
            return all_cases
        filter_text = filter_text.lower().strip()
        filtered = []
        for case in all_cases:
            for v in case.values():
                if v and filter_text in str(v).lower():
                    filtered.append(case)
                    break
        return filtered

    def load_next_lazy_page(self):
        """Load the next page of cases into the Treeview."""
        if self._lazy_loading:
            return
        self._lazy_loading = True
        start = self._lazy_offset
        end = min(start + self.LAZY_PAGE_SIZE, self._lazy_total)
        cases = self._lazy_cases[start:end]
        visible_columns = self.get_visible_treeview_columns()
        for case in cases:
            values = [case.get(col, "") for col in self.tree["columns"]]
            self.tree.insert("", "end", values=values)
        self._lazy_offset = end
        self._lazy_loading = False

    def on_treeview_scroll(self, *args):
        """Callback for Treeview vertical scroll. Loads more data if near bottom."""
        self.tree.yview(*args)
        # Check if near bottom
        if self.tree.yview()[1] > 0.95 and self._lazy_offset < self._lazy_total:
            self.load_next_lazy_page()
    def show_total_case_summary(self):
        """Show dialog for date range summary options (clickable inputs) and generate a summary report filtered by start_date."""
        import tkinter as tk
        from tkinter import Toplevel, Label, Button, StringVar, IntVar, Checkbutton, Spinbox
        from datetime import datetime, timedelta
        win = Toplevel(self.root)
        win.title("Date Range Options")
        win.grab_set()
        # Date range (use DateEntry widgets)
        Label(win, text="Start Date:").grid(row=0, column=0, sticky='w', padx=10, pady=(10,2))
        start_var = StringVar(value="")
        start_entry = DateEntry(win, width=12, dateformat='%m-%d-%Y')
        start_entry.grid(row=0, column=1, padx=5, pady=(10,2))
        Label(win, text="End Date:").grid(row=1, column=0, sticky='w', padx=10, pady=2)
        end_var = StringVar(value="")
        end_entry = DateEntry(win, width=12, dateformat='%m-%d-%Y')
        end_entry.grid(row=1, column=1, padx=5, pady=2)
        # Recent activity
        recent_var = IntVar(value=0)
        Label(win, text="Recent Activity (days):").grid(row=2, column=0, sticky='w', padx=10, pady=2)
        recent_days_var = StringVar(value="7")
        recent_spin = Spinbox(win, from_=1, to=365, textvariable=recent_days_var, width=5)
        recent_spin.grid(row=2, column=1, padx=5, pady=2)
        recent_check = Checkbutton(win, text="Show only recent cases", variable=recent_var)
        recent_check.grid(row=2, column=2, padx=5, pady=2)
        # Output format
        Label(win, text="Output format:").grid(row=3, column=0, sticky='w', padx=10, pady=(10,2))
        fmt_var = StringVar(value="PDF")
        Button(win, text="PDF", command=lambda: fmt_var.set("PDF")).grid(row=3, column=1, sticky='w')
        Button(win, text="Excel (XLSX)", command=lambda: fmt_var.set("XLSX")).grid(row=3, column=2, sticky='w')
        def parse_dateentry(de):
            try:
                # DateEntry returns a datetime.date or string depending on widget; handle both
                val = de.entry.get().strip() if hasattr(de, 'entry') else str(de.get()).strip()
                if not val:
                    return None
                # Accept both MM-DD-YYYY and YYYY-MM-DD
                for fmt in ('%m-%d-%Y', '%m/%d/%Y', '%Y-%m-%d'):
                    try:
                        return datetime.strptime(val, fmt)
                    except Exception:
                        continue
                return None
            except Exception:
                return None
        def do_summary():
            # Parse date range from DateEntry widgets
            sd = parse_dateentry(start_entry)
            ed = parse_dateentry(end_entry)
            start_date = sd.strftime('%Y-%m-%d') if sd else ""
            end_date = ed.strftime('%Y-%m-%d') if ed else ""
            cases = get_all_cases_db()
            filtered = []
            for c in cases:
                start = c.get('start_date')
                if not start:
                    continue
                try:
                    dt = datetime.strptime(str(start)[:10], '%Y-%m-%d')
                except Exception:
                    # Try alternate common format in DB (MM-DD-YYYY)
                    try:
                        dt = datetime.strptime(str(start)[:10], '%m-%d-%Y')
                    except Exception:
                        continue
                if sd and dt < sd:
                    continue
                if ed and dt > ed:
                    continue
                filtered.append(c)
            # Recent activity filter (based on start_date only)
            if recent_var.get():
                try:
                    days = int(recent_days_var.get())
                    cutoff = datetime.now() - timedelta(days=days)
                    def row_dt(c):
                        val = c.get('start_date')
                        for fmt in ('%Y-%m-%d', '%m-%d-%Y', '%m/%d/%Y'):
                            try:
                                return datetime.strptime(str(val)[:10], fmt)
                            except Exception:
                                continue
                        return None
                    filtered = [c for c in filtered if (row_dt(c) and row_dt(c) >= cutoff)]
                except Exception:
                    pass
            if not filtered:
                Messagebox.show_info("Oops!", "No cases found for the selected range/criteria.")
                return
            if fmt_var.get() == "PDF":
                self.export_total_case_summary_pdf(filtered, start_date, end_date, recent_var.get(), recent_days_var.get())
            else:
                self.export_total_case_summary_xlsx(filtered, start_date, end_date, recent_var.get(), recent_days_var.get())
            win.destroy()
        Button(win, text="Generate Summary", command=do_summary).grid(row=4, column=0, columnspan=3, pady=15)

    # --- ReportLab Helpers: header layout, margins, and dynamic title sizing ---
    def _pdf_calc_title_font_size(self, title_text, max_width, base_size=20, min_size=10, font_name="Helvetica-Bold"):
        """Reduce font size until the title fits on one line within max_width.
        Returns the chosen font size (int)."""
        size = base_size
        # Safety cap on iterations
        for _ in range(20):
            width = stringWidth(title_text, font_name, size)
            if width <= max_width or size <= min_size:
                break
            size -= 1
        return max(size, min_size)

    def _pdf_build_header(self, title_text, doc_width, has_logo=True, right_logo_width=1.0*inch, right_logo_height=1.0*inch, left_info_lines=None):
        """Return a list of Flowables forming a header with left info, centered/left title, and right-aligned logo.
        - doc_width: page width minus margins (available content width)
        - title is auto-sized to fit on one line
        - logo sits at the top-right
        """
        elements = []
        styles = getSampleStyleSheet()
        # Left header info as a small table
        left_info_lines = left_info_lines or []
        left_table = None
        if left_info_lines:
            left_rows = [[Paragraph(line, styles["Normal"])] for line in left_info_lines]
            left_table = Table(left_rows, hAlign='LEFT')
        # Prepare logo flowable if available
        logo_flowable = None
        if has_logo and os.path.exists(LOGO_FILENAME):
            try:
                logo_flowable = ReportLabImage(LOGO_FILENAME, width=right_logo_width, height=right_logo_height)
            except Exception:
                logo_flowable = None
        # Compute dynamic title font size
        title_max_width = doc_width - (right_logo_width + 12 if logo_flowable else 0) - 6
        title_font_size = self._pdf_calc_title_font_size(title_text, title_max_width)
        title_style = styles['Title']
        # Create a shallow copy of style to override size
        title_style_custom = copy_style(title_style)
        title_style_custom.fontSize = title_font_size
        title_style_custom.leading = max(title_font_size + 2, 14)
        title_para = Paragraph(f"<b>{title_text}</b>", title_style_custom)

        # Build a 2-col table: left block (left info above title), right logo
        # Left block stacks: left info table (optional) then title
        left_block = []
        if left_table:
            left_block.append(left_table)
            left_block.append(Spacer(1, 4))
        left_block.append(title_para)
        left_col = Table([[b] for b in left_block], hAlign='LEFT')

        if logo_flowable:
            header_table = Table(
                [[left_col, logo_flowable]],
                colWidths=[doc_width - right_logo_width - 6, right_logo_width],
            )
            header_table.setStyle(TableStyle([
                ("VALIGN", (0,0), (-1,-1), "TOP"),
                ("ALIGN", (0,0), (0,0), "LEFT"),
                ("ALIGN", (1,0), (1,0), "RIGHT"),
                ("LEFTPADDING", (0,0), (-1,-1), 0),
                ("RIGHTPADDING", (0,0), (-1,-1), 0),
                ("TOPPADDING", (0,0), (-1,-1), 0),
                ("BOTTOMPADDING", (0,0), (-1,-1), 2),
            ]))
            elements.append(header_table)
        else:
            # No logo, just left col
            elements.append(left_col)
        elements.append(Spacer(1, 10))
        return elements

    def _pdf_footer(self, canvas, doc, generated_by: str = "", generated_date: str = ""):
        """Draw a footer: left = Generated by/date, right = Page X."""
        from reportlab.lib.colors import black
        try:
            canvas.saveState()
            canvas.setFont("Helvetica", 8)
            canvas.setFillColor(black)
            left_text = f"Generated by {generated_by or ''} on {generated_date}"
            # Use same margins as documents (0.4 inch left/right, 0.5 inch bottom)
            from reportlab.lib.units import inch as _inch_unit
            page_num = getattr(canvas, 'getPageNumber', lambda: 0)()
            canvas.drawString(0.4*_inch_unit, 0.35*_inch_unit, left_text)
            page_text = f"Page {page_num}"
            try:
                page_width = canvas._pagesize[0]
            except Exception:
                page_width = doc.pagesize[0]
            canvas.drawRightString(page_width - 0.4*_inch_unit, 0.35*_inch_unit, page_text)
        finally:
            try:
                canvas.restoreState()
            except Exception:
                pass

    def _make_pdf_canvasmaker(self, generated_by: str = "", generated_date: str = ""):
        """Return a canvasmaker that draws 'Generated by' and 'Page X of Y' footers for ReportLab doc.build."""
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.colors import black
        from reportlab.lib.units import inch

        gen_by = generated_by or ""
        gen_dt = generated_date or ""

        class NumberedCanvas(rl_canvas.Canvas):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._saved_page_states = []

            def showPage(self):
                self._saved_page_states.append(dict(self.__dict__))
                self._startPage()

            def save(self):
                total_pages = len(self._saved_page_states)
                for state in self._saved_page_states:
                    self.__dict__.update(state)
                    self._draw_footer(total_pages)
                    super().showPage()
                super().save()

            def _draw_footer(self, total_pages: int):
                try:
                    self.saveState()
                    self.setFont("Helvetica", 8)
                    self.setFillColor(black)
                    # Left: generated info
                    left_text = f"Generated by {gen_by} on {gen_dt}"
                    self.drawString(0.4*inch, 0.35*inch, left_text)
                    # Right: page X of Y
                    page_text = f"Page {self.getPageNumber()} of {total_pages}"
                    page_width = getattr(self, "_pagesize", (612, 792))[0]
                    self.drawRightString(page_width - 0.4*inch, 0.35*inch, page_text)
                finally:
                    try:
                        self.restoreState()
                    except Exception:
                        pass

        def _factory(filename, pagesize, **kwargs):
            return NumberedCanvas(filename, pagesize, **kwargs)

        return _factory

    def export_total_case_summary_pdf(self, cases, start_date, end_date, recent_only, recent_days):
        from tkinter import filedialog
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.lib.units import inch
        filename = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[["PDF files", "*.pdf"]], title="Save Total Summary PDF")
        if not filename:
            return
        doc = SimpleDocTemplate(
            filename,
            pagesize=letter,
            leftMargin=0.4*inch,
            rightMargin=0.4*inch,
            topMargin=0.5*inch,
            bottomMargin=0.5*inch,
        )
        elements = []
        styles = getSampleStyleSheet()
        header_info = self.get_report_header_info()
        header_lines = [
            f"Name: {header_info.get('Name','')}",
            f"Agency: {header_info.get('Agency','')}",
            f"Division: {header_info.get('Division','')}",
            f"Date: {header_info.get('Date','')}"
        ]
        title = "Total Lab Summary"
        if start_date or end_date:
            title += f" ({start_date or '...'} to {end_date or '...'})"
        if recent_only:
            title += f" (Recent {recent_days} days)"
        elements.extend(self._pdf_build_header(
            title_text=title,
            doc_width=doc.width,
            has_logo=True,
            right_logo_width=1.1*inch,
            right_logo_height=1.1*inch,
            left_info_lines=header_lines,
        ))
        total_cases = len(cases)
        total_gb = sum(float(c.get('volume_size_gb') or 0) for c in cases)
        total_tb = total_gb / 1024 if total_gb > 999 else None
        elements.append(Paragraph(f"<b>Total Devices:</b> {total_cases}", styles["Normal"]))
        elements.append(Paragraph(
            f"<b>Total Volume:</b> {total_tb:.2f} TB" if total_tb else f"<b>Total Volume:</b> {total_gb:.2f} GB",
            styles["Normal"],
        ))
        def breakdown(field):
            d = {}
            for c in cases:
                v = (c.get(field) or '').strip()
                if v:
                    d[v] = d.get(v, 0) + 1
            return sorted(d.items(), key=lambda x: x[1], reverse=True)
        for field, label in [("examiner", "Examiner"), ("agency", "Agency"), ("offense_type", "Offense Type"), ("device_type", "Device Type")]:
            items = breakdown(field)
            if items:
                elements.append(Spacer(1, 8))
                elements.append(Paragraph(f"<b>{label} Breakdown:</b>", styles["Normal"]))
                t = Table([[k, v] for k, v in items], colWidths=[2.5*inch, 1*inch])
                t.setStyle(TableStyle([
                    ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
                    ("ALIGN", (0,0), (-1,-1), "LEFT"),
                    ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
                    ("FONTSIZE", (0,0), (-1,-1), 9),
                    ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
                ]))
                elements.append(t)
        if recent_only:
            elements.append(Spacer(1, 12))
            elements.append(Paragraph(f"<b>Recent Cases (last {recent_days} days):</b>", styles["Normal"]))
            case_rows = []
            for c in cases:
                case_rows.append([
                    c.get('case_number', ''),
                    format_date_str_for_display(c.get('created_at', '')),
                    c.get('examiner', ''),
                    c.get('offense_type', ''),
                    c.get('volume_size_gb', '')
                ])
            t = Table([["Case #", "Created", "Examiner", "Offense", "Vol (GB)"]] + case_rows,
                      colWidths=[1*inch, 1.1*inch, 1.2*inch, 2.2*inch, 0.8*inch])
            t.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
                ("ALIGN", (0,0), (-1,-1), "LEFT"),
                ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
                ("FONTSIZE", (0,0), (-1,-1), 8),
                ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ]))
            elements.append(t)
        gen_info = self.get_report_header_info()
        gen_name = gen_info.get('Name','')
        gen_date = datetime.now().strftime('%Y-%m-%d %H:%M')
        canvasmaker = self._make_pdf_canvasmaker(gen_name, gen_date)
        doc.build(elements, canvasmaker=canvasmaker)
        self._show_report_saved_dialog(filename)

    def export_total_case_summary_xlsx(self, cases, start_date, end_date, recent_only, recent_days):
        from tkinter import filedialog
        import pandas as pd
        filename = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")], title="Save Total Summary Excel")
        if not filename:
            return
        total_cases = len(cases)
        total_gb = sum(float(c.get('volume_size_gb') or 0) for c in cases)
        total_tb = total_gb / 1024 if total_gb > 999 else None
        df_summary = pd.DataFrame({
            "Total Cases": [total_cases],
            "Total Volume (GB)": [total_gb],
            "Total Volume (TB)": [total_tb if total_tb else '']
        })
        def breakdown(field):
            d = {}
            for c in cases:
                v = (c.get(field) or '').strip()
                if v:
                    d[v] = d.get(v, 0) + 1
            return sorted(d.items(), key=lambda x: x[1], reverse=True)
        with pd.ExcelWriter(filename) as writer:
            df_summary.to_excel(writer, sheet_name="Summary", index=False)
            for field, label in [("examiner", "Examiner"), ("agency", "Agency"), ("offense_type", "Offense Type"), ("device_type", "Device Type")]:
                items = breakdown(field)
                if items:
                    pd.DataFrame(items, columns=[label, "Count"]).to_excel(writer, sheet_name=label, index=False)
            if recent_only:
                rows = []
                for c in cases:
                    rows.append({
                        "Case #": c.get('case_number', ''),
                        "Created": format_date_str_for_display(c.get('created_at', '')),
                        "Examiner": c.get('examiner', ''),
                        "Offense": c.get('offense_type', ''),
                        "Vol (GB)": c.get('volume_size_gb', '')
                    })
                pd.DataFrame(rows).to_excel(writer, sheet_name=f"Recent_{recent_days}d", index=False)
        self._show_report_saved_dialog(filename)

    def show_all_cases_summary(self):
        """Generate overall summary using all cases without date filtering."""
        import tkinter as tk
        from tkinter import Toplevel, Label, Button, StringVar
        win = Toplevel(self.root)
        win.title("All Cases (Summary)")
        win.grab_set()
        Label(win, text="Export format:").grid(row=0, column=0, padx=10, pady=(10,5), sticky='w')
        fmt_var = StringVar(value="PDF")
        Button(win, text="PDF", command=lambda: fmt_var.set("PDF")).grid(row=0, column=1, padx=5, pady=(10,5), sticky='w')
        Button(win, text="Excel (XLSX)", command=lambda: fmt_var.set("XLSX")).grid(row=0, column=2, padx=5, pady=(10,5), sticky='w')
        def do_export():
            cases = get_all_cases_db()
            if not cases:
                Messagebox.show_info("Summary", "No cases available.")
                return
            if fmt_var.get() == "PDF":
                self.export_total_case_summary_pdf(cases, "", "", 0, "")
            else:
                self.export_total_case_summary_xlsx(cases, "", "", 0, "")
            win.destroy()
        Button(win, text="Generate Summary", command=do_export).grid(row=1, column=0, columnspan=3, pady=12)
    def show_case_summary_report(self):
        """Generate a one-page PDF summary for the selected case."""
        from tkinter import filedialog
        if not self.tree.selection() or len(self.tree.selection()) != 1:
            Messagebox.show_info("Must select one case", "Please select exactly one case in the table.")
            return
        selected_id = self.tree.item(self.tree.selection()[0])['values'][0]
        case = get_case_by_id_db(selected_id)
        if not case:
            Messagebox.show_error("Case Summary", "Could not retrieve case details.")
            return
        filename = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF files", "*.pdf")], title="Save Case Summary PDF")
        if not filename:
            return
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.lib.units import inch
        doc = SimpleDocTemplate(
            filename,
            pagesize=letter,
            leftMargin=0.4*inch,
            rightMargin=0.4*inch,
            topMargin=0.5*inch,
            bottomMargin=0.5*inch,
        )
        elements = []
        styles = getSampleStyleSheet()
        elements.extend(self._pdf_build_header(
            title_text="Case Summary Report",
            doc_width=doc.width,
            has_logo=True,
            right_logo_width=1.1*inch,
            right_logo_height=1.1*inch,
            left_info_lines=[],
        ))
        field_map = [(k, v['text']) for k, v in self.tree_columns_config.items() if k != 'id']
        data = []
        for key, label in field_map:
            val = case.get(key, "")
            if key in ('start_date', 'end_date', 'created_at'):
                val = format_date_str_for_display(val)
            elif key == 'fpr_complete':
                val = format_bool_int(val)
            elif key == 'data_recovered':
                val = val if val else ""
            data.append([label, val])
        table = Table(data, colWidths=[2.5*inch, 4.5*inch])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (0,-1), colors.whitesmoke),
            ("ALIGN", (0,0), (-1,-1), "LEFT"),
            ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
            ("FONTSIZE", (0,0), (-1,-1), 10),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
        ]))
        elements.append(table)
        gen_info = self.get_report_header_info()
        gen_name = gen_info.get('Name','')
        gen_date = datetime.now().strftime('%Y-%m-%d %H:%M')
        canvasmaker = self._make_pdf_canvasmaker(gen_name, gen_date)
        doc.build(elements, canvasmaker=canvasmaker)
        self._show_report_saved_dialog(filename)
    def show_custom_report_builder(self):
        """Show a dialog for building a custom report from selected columns and export as PDF/XLSX."""
        import tkinter as tk
        from tkinter import Toplevel, Checkbutton, IntVar, Button, Label, StringVar, Radiobutton, Entry
        all_columns = [k for k in self.tree_columns_config.keys() if k != 'id']
        col_labels = {k: self.tree_columns_config[k]['text'] for k in all_columns}
        # Default: use currently visible columns
        default_cols = set(self.get_visible_treeview_columns())
        win = Toplevel(self.root)
        win.title("Custom Report Builder")
        win.grab_set()
        vars = {}
        Label(win, text="Select columns to include:", font=("Arial", 11, "bold")).grid(row=0, column=0, sticky='w', padx=10, pady=(10,2))
        for i, col in enumerate(all_columns):
            var = IntVar(value=1 if col in default_cols else 0)
            cb = Checkbutton(win, text=col_labels[col], variable=var)
            cb.grid(row=i+1, column=0, sticky='w', padx=20, pady=2)
            vars[col] = var
        # Output format
        Label(win, text="Output format:").grid(row=len(all_columns)+1, column=0, sticky='w', padx=10, pady=(10,2))
        fmt_var = StringVar(value="PDF")
        Radiobutton(win, text="PDF", variable=fmt_var, value="PDF").grid(row=len(all_columns)+2, column=0, sticky='w', padx=20)
        Radiobutton(win, text="Excel (XLSX)", variable=fmt_var, value="XLSX").grid(row=len(all_columns)+3, column=0, sticky='w', padx=20)
        # Report title input
        Label(win, text="Report Title:").grid(row=len(all_columns)+4, column=0, sticky='w', padx=10, pady=(10,2))
        title_var = StringVar(value="Custom Report")
        Entry(win, textvariable=title_var, width=40).grid(row=len(all_columns)+5, column=0, sticky='w', padx=20)
        # Filter: all, filtered, or selected rows
        filter_var = StringVar(value="all")
        Label(win, text="Rows to include:").grid(row=len(all_columns)+6, column=0, sticky='w', padx=10, pady=(10,2))
        Radiobutton(win, text="All cases", variable=filter_var, value="all").grid(row=len(all_columns)+7, column=0, sticky='w', padx=20)
        Radiobutton(win, text="Filtered (current search)", variable=filter_var, value="filtered").grid(row=len(all_columns)+8, column=0, sticky='w', padx=20)
        Radiobutton(win, text="Selected rows only", variable=filter_var, value="selected").grid(row=len(all_columns)+9, column=0, sticky='w', padx=20)
        def do_export():
            selected_cols = [col for col, v in vars.items() if v.get()]
            if not selected_cols:
                messagebox.showerror("Error", "Select at least one column.")
                return
            # Get data
            if filter_var.get() == "all":
                cases = get_all_cases_db()
            elif filter_var.get() == "filtered":
                cases = getattr(self, '_last_filtered_cases', get_all_cases_db())
            else:  # selected
                selected_ids = [self.tree.item(i)['values'][0] for i in self.tree.selection()]
                all_cases = get_all_cases_db()
                cases = [c for c in all_cases if c.get('id') in selected_ids]
            # Totals: Devices and Total Volume (GB)
            total_devices = len(cases)
            def _to_float(x):
                try:
                    return float(x)
                except Exception:
                    return 0.0
            total_volume_gb = sum(_to_float(c.get('volume_size_gb')) for c in cases)
            # Build rows
            rows = []
            for case in cases:
                row = []
                for col in selected_cols:
                    val = case.get(col, "")
                    # Format for display if needed
                    if col in ('start_date', 'end_date', 'created_at'):
                        val = format_date_str_for_display(val)
                    elif col in ('fpr_complete',):
                        val = format_bool_int(val)
                    row.append(val)
                rows.append(row)
            # Header row
            headers = [col_labels[c] for c in selected_cols]
            if fmt_var.get() == "PDF":
                self.export_custom_report_pdf(
                    headers, rows,
                    title=title_var.get().strip() or "Custom Report",
                    total_devices=total_devices,
                    total_volume_gb=total_volume_gb,
                )
            else:
                self.export_custom_report_xlsx(headers, rows)
            win.destroy()
        btn = Button(win, text="Export", command=do_export)
        btn.grid(row=len(all_columns)+10, column=0, pady=15)

    def export_custom_report_pdf(self, headers, rows, title="Custom Report", total_devices=None, total_volume_gb=None):
        """Export custom report as PDF with improved formatting and word wrapping."""
        from tkinter import filedialog
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_LEFT
        from datetime import datetime
        
        # Prompt for header info if not set
        info = self.get_report_header_info()
        if not any(info.values()):
            self.prompt_report_header_info()
            info = self.get_report_header_info()
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".pdf", 
            filetypes=[("PDF files", "*.pdf")], 
            title="Save PDF Report"
        )
        if not filename:
            return

        try:
            # Use landscape to maximize horizontal space and ensure wide tables fit
            num_cols = len(headers)
            use_landscape = True
            pagesize = landscape(letter)
            
            doc = SimpleDocTemplate(
                filename,
                pagesize=pagesize,
                leftMargin=0.4*inch,
                rightMargin=0.4*inch,
                topMargin=0.5*inch,
                bottomMargin=0.5*inch,
            )
            
            styles = getSampleStyleSheet()
            
            # Calculate column widths dynamically based on content and actual doc width
            page_width = doc.width
            
            # Calculate column width preferences based on content analysis
            col_weights = []
            for i, header in enumerate(headers):
                # Analyze content to determine optimal width
                header_len = len(str(header))
                max_content_len = header_len
                
                for row in rows:
                    if i < len(row):
                        content_len = len(str(row[i]))
                        max_content_len = max(max_content_len, content_len)
                
                # Weight based on content length, with min/max limits
                weight = max(0.5, min(3.0, max_content_len / 10))
                col_weights.append(weight)
            
            # Calculate actual column widths (bounded by min/max and sum to available width)
            total_weight = sum(col_weights)
            col_widths = []
            # Dynamic bounds based on number of columns
            if num_cols >= 12:
                min_width, max_width = 0.3 * inch, 1.6 * inch
            elif num_cols >= 9:
                min_width, max_width = 0.35 * inch, 2.0 * inch
            else:
                min_width, max_width = 0.4 * inch, 2.3 * inch
            for weight in col_weights:
                width = (weight / total_weight) * page_width
                col_widths.append(max(min_width, min(width, max_width)))
            # Ensure total width does not exceed available page width
            total_width = sum(col_widths)
            if total_width > page_width and total_width > 0:
                scale = page_width / total_width
                col_widths = [w * scale for w in col_widths]

            # Pick font sizes based on column count to ensure fit
            if num_cols >= 12:
                header_font, data_font = 6, 6
                top_pad, bottom_pad, side_pad = 2, 4, 2
            elif num_cols >= 9:
                header_font, data_font = 7, 6
                top_pad, bottom_pad, side_pad = 2, 4, 2
            else:
                header_font, data_font = 8, 7
                top_pad, bottom_pad, side_pad = 3, 6, 3

            # Create paragraph styles for header and cells with dynamic sizing
            cell_style = ParagraphStyle(
                'CellStyle',
                parent=styles['Normal'],
                fontSize=data_font,
                leading=max(data_font + 1, data_font),
                leftIndent=0,
                rightIndent=0,
                spaceAfter=0,
                spaceBefore=0,
                alignment=TA_LEFT,
                wordWrap='LTR'
            )
            header_para_style = ParagraphStyle(
                'HeaderCellStyle',
                parent=styles['Normal'],
                fontName='Helvetica-Bold',
                fontSize=header_font,
                leading=max(header_font + 1, header_font),
                alignment=TA_LEFT,
            )
            
            # Convert all table data to Paragraphs for proper word wrapping
            table_data = []
            
            # Process headers
            header_row = []
            for header in headers:
                header_row.append(Paragraph(str(header), header_para_style))
            table_data.append(header_row)
            
            # Process data rows
            for row in rows:
                data_row = []
                for cell in row:
                    # Convert all cell content to strings and wrap in Paragraphs
                    cell_content = str(cell) if cell is not None else ""
                    data_row.append(Paragraph(cell_content, cell_style))
                table_data.append(data_row)
            
            # Create table with improved settings
            table = Table(
                table_data, 
                colWidths=col_widths, 
                repeatRows=1,
                splitByRow=True
            )
            
            # Apply improved styling
            table.setStyle(TableStyle([
                # Header styling - red background with white text
                ("BACKGROUND", (0,0), (-1,0), colors.red),
                ("TEXTCOLOR", (0,0), (-1,0), colors.white),
                ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
                ("FONTSIZE", (0,0), (-1,0), header_font),
                ("ALIGN", (0,0), (-1,0), "CENTER"),
                ("VALIGN", (0,0), (-1,0), "MIDDLE"),
                
                # Data row styling
                ("BACKGROUND", (0,1), (-1,-1), colors.white),
                ("TEXTCOLOR", (0,1), (-1,-1), colors.black),
                ("FONTNAME", (0,1), (-1,-1), "Helvetica"),
                ("FONTSIZE", (0,1), (-1,-1), data_font),
                ("ALIGN", (0,1), (-1,-1), "LEFT"),
                ("VALIGN", (0,1), (-1,-1), "TOP"),
                
                # Grid and borders
                ("GRID", (0,0), (-1,-1), 0.5, colors.black),
                ("LINEBELOW", (0,0), (-1,0), 1, colors.red),
                
                # Padding for better spacing
                ("LEFTPADDING", (0,0), (-1,-1), side_pad),
                ("RIGHTPADDING", (0,0), (-1,-1), side_pad),
                ("TOPPADDING", (0,0), (-1,-1), top_pad),
                ("BOTTOMPADDING", (0,0), (-1,-1), bottom_pad),
                
                # Word wrapping
                ("WORDWRAP", (0,0), (-1,-1), True),
                ("SPLITLONGWORDS", (0,0), (-1,-1), True),
            ]))
            
            # Add alternating row colors for better readability
            for i in range(1, len(table_data)):
                if i % 2 == 0:
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, i), (-1, i), colors.Color(0.95, 0.95, 0.95)),
                    ]))
            
            elements = []
            
            # Header with logo top-right and single-line title
            header_info = self.get_report_header_info()
            now_str = datetime.now().strftime('%Y-%m-%d')
            header_lines = [
                f"Name: {header_info.get('Name','')}",
                f"Agency: {header_info.get('Agency','')}",
                f"Division: {header_info.get('Division','')}",
                f"Date: {now_str}"
            ]
            elements.extend(self._pdf_build_header(
                title_text=title or "Custom Report",
                doc_width=doc.width,
                has_logo=True,
                right_logo_width=1.1*inch,
                right_logo_height=1.1*inch,
                left_info_lines=header_lines,
            ))
            # Optional totals (Devices and Total Volume) below header
            if total_devices is not None or total_volume_gb is not None:
                totals_data = [[
                    Paragraph(f"Devices: <b>{total_devices or 0}</b>", styles['Normal']),
                    Paragraph(f"Total Volume: <b>{(round(total_volume_gb, 2) if isinstance(total_volume_gb, (int, float)) else 0)} GB</b>", styles['Normal'])
                ]]
                totals_table = Table(totals_data, colWidths=[doc.width/2.0, doc.width/2.0])
                totals_table.setStyle(TableStyle([
                    ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
                    ("FONTSIZE", (0,0), (-1,-1), 9),
                    ("ALIGN", (0,0), (0,0), "LEFT"),
                    ("ALIGN", (1,0), (1,0), "RIGHT"),
                    ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                    ("BOTTOMPADDING", (0,0), (-1,-1), 6),
                ]))
                elements.append(totals_table)
                elements.append(Spacer(1, 6))
            
            elements.append(table)
            # Footer with page numbers 'X of Y' and generator info
            gen_info = self.get_report_header_info()
            gen_name = gen_info.get('Name','')
            gen_date = datetime.now().strftime('%Y-%m-%d %H:%M')
            canvasmaker = self._make_pdf_canvasmaker(gen_name, gen_date)
            doc.build(elements, canvasmaker=canvasmaker)
            self._show_report_saved_dialog(filename)
            
        except Exception as e:
            logging.error(f"Error generating custom PDF report: {e}")
            Messagebox.show_error("Error", f"Failed to generate custom PDF report: {e}")

    def export_custom_report_xlsx(self, headers, rows):
        from tkinter import filedialog
        import pandas as pd
        from datetime import datetime
        # Prompt for header info if not set
        info = self.get_report_header_info()
        if not any(info.values()):
            self.prompt_report_header_info()
            info = self.get_report_header_info()
        filename = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")], title="Save Excel Report")
        if not filename:
            return
        df = pd.DataFrame(rows, columns=headers)
        # Add header info as a separate sheet
        header_info = self.get_report_header_info()
        now_str = datetime.now().strftime('%Y-%m-%d')
        header_dict = {
            'Name': header_info.get('Name',''),
            'Agency': header_info.get('Agency',''),
            'Division': header_info.get('Division',''),
            'Date': now_str
        }
        with pd.ExcelWriter(filename) as writer:
            df.to_excel(writer, index=False, sheet_name='Report Data')
            pd.DataFrame([header_dict]).to_excel(writer, index=False, sheet_name='Header Info')
        self._show_report_saved_dialog(filename)
    def show_report_header_info_settings(self):
        self.prompt_report_header_info()
    def get_visible_treeview_columns(self):
        """Return the list of visible columns for the Treeview, based on user preferences."""
        # Always hide 'id'
        all_columns = [key for key in self.tree_columns_config.keys() if key != 'id']
        visible = get_user_pref('treeview_columns')
        if visible and isinstance(visible, list):
            # Only show columns that still exist
            return [col for col in visible if col in all_columns]
        return all_columns

    def set_visible_treeview_columns(self, columns):
        """Save the list of visible columns for the Treeview."""
        set_user_pref('treeview_columns', columns)
        self.refresh_data_view()

    def show_column_selector(self):
        """Show a dialog to let the user select which columns are visible in the View Data tab."""
        import tkinter as tk
        from tkinter import Toplevel, Checkbutton, IntVar, Button, Label
        # Get all columns except 'id'
        all_columns = [k for k in self.tree_columns_config.keys() if k != 'id']
        current = set(self.get_visible_treeview_columns())
        win = Toplevel(self.root)
        win.title("Select Columns to Display")
        win.grab_set()
        win.rowconfigure(0, weight=1)
        win.columnconfigure(0, weight=1)
        content = tk.Frame(win)
        content.grid(row=0, column=0, sticky='nsew', padx=10, pady=10)
        content.rowconfigure(0, weight=1)
        content.columnconfigure(0, weight=1)
        vars = {}
        for i, col in enumerate(all_columns):
            var = IntVar(value=1 if col in current else 0)
            cb = Checkbutton(content, text=self.tree_columns_config[col]['text'], variable=var)
            cb.grid(row=i, column=0, sticky='w', padx=5, pady=2)
            vars[col] = var
        def apply():
            selected = [col for col, v in vars.items() if v.get()]
            if not selected:
                messagebox.showerror("Error", "At least one column must be selected.")
                return
            self.set_visible_treeview_columns(selected)
            win.destroy()
        btn = Button(content, text="Apply", command=apply)
        btn.grid(row=len(all_columns), column=0, pady=10, sticky='ew')
        content.rowconfigure(len(all_columns), weight=1)

    # --- Undo/Redo for View Data Editing ---
    def init_view_edit_history(self):
        """Initializes the undo/redo history stacks for View Data editing."""
        self._view_edit_undo_stack = []
        self._view_edit_redo_stack = []

    def push_view_edit_history(self, old_data, new_data):
        """Pushes an edit action to the undo stack and clears the redo stack."""
        self._view_edit_undo_stack.append((old_data, new_data))
        self._view_edit_redo_stack.clear()

    def undo_view_edit(self):
        """Undo the last edit in View Data, if possible."""
        if not hasattr(self, '_view_edit_undo_stack') or not self._view_edit_undo_stack:
            Messagebox.show_info("Undo", "Nothing to undo.")
            return
        old_data, new_data = self._view_edit_undo_stack.pop()
        # Save redo info
        self._view_edit_redo_stack.append((old_data, new_data))
        # Restore old data in DB
        if old_data and 'id' in old_data:
            update_case_db(old_data['id'], old_data)
            self.refresh_data_view()
            self.update_status("Undo: Edit reverted.")
        else:
            self.update_status("Undo failed: No valid data.")

    def redo_view_edit(self):
        """Redo the last undone edit in View Data, if possible."""
        if not hasattr(self, '_view_edit_redo_stack') or not self._view_edit_redo_stack:
            Messagebox.show_info("Redo", "Nothing to redo.")
            return
        old_data, new_data = self._view_edit_redo_stack.pop()
        # Save undo info
        self._view_edit_undo_stack.append((old_data, new_data))
        # Re-apply new data in DB
        if new_data and 'id' in new_data:
            update_case_db(new_data['id'], new_data)
            self.refresh_data_view()
            self.update_status("Redo: Edit re-applied.")
        else:
            self.update_status("Redo failed: No valid data.")

    def load_map_markers(self):
        """Load map markers for each unique city/state in the case log, showing offenses on click. Async geocoding for uncached locations."""
        import threading
        if not self.map_widget:
            if self.map_status_label:
                self.map_status_label.config(text="Map status: Map widget not available")
            return
        if hasattr(self.map_widget, 'delete_all_markers'):
            self.map_widget.delete_all_markers()
        cases = get_all_cases_db()
        # Group cases by (city, state)
        grouped = {}
        for case in cases:
            city = (case.get('city_of_offense') or '').strip()
            state = (case.get('state_of_offense') or '').strip()
            if not city or not state:
                continue
            key = (city, state)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(case)
        logging.info(f"[MapMarkers] Found {len(grouped)} unique city/state locations.")
        self.map_markers = {}
        self._grouped_cases_by_location = grouped
        # Prepare geocoding queue for uncached locations
        self.geocoding_queue = queue.Queue()
        self._pending_marker_locations = []
        for (city, state) in grouped:
            location_key = f"{city}|{state}"
            coords = get_cached_location_db(location_key)
            if coords:
                # Place marker immediately
                self._place_map_marker(city, state, coords)
            else:
                self._pending_marker_locations.append((city, state))
                self.geocoding_queue.put((city, state))
        if self.map_status_label:
            self.map_status_label.config(text=f"Map status: {len(self.map_markers)} cached, {len(self._pending_marker_locations)} to geocode")
        # Start geocoding thread if needed
        if self._pending_marker_locations:
            if not hasattr(self, 'geocoding_thread') or self.geocoding_thread is None or not self.geocoding_thread.is_alive():
                self.geocoding_thread = threading.Thread(target=self._geocode_locations_worker, daemon=True)
                self.geocoding_thread.start()
                self.processing_queue = True
                self._geocoding_after_id = self.root.after(500, self._process_geocoding_results)
            else:
                self.processing_queue = True
                self._geocoding_after_id = self.root.after(500, self._process_geocoding_results)
        else:
            self.processing_queue = False
            if self.map_status_label:
                self.map_status_label.config(text=f"Map status: {len(self.map_markers)} locations loaded (all cached)")

    def _place_map_marker(self, city, state, coords):
        """Helper to place a marker on the map for a city/state with given coords."""
        try:
            lat, lon = coords
            city_cases = self._grouped_cases_by_location.get((city, state), [])
            offenses = sorted(set((c.get('offense_type') or '').strip() for c in city_cases if c.get('offense_type')))
            offense_str = ', '.join(offenses) if offenses else 'No offenses recorded'
            info_text = f"{city}, {state}\nOffense Types: {offense_str}"
            marker_icon = getattr(self, 'marker_icon_tk_map', None)
            marker = self.map_widget.set_marker(
                lat, lon,
                text="",
                icon=marker_icon if marker_icon else DEFAULT_MARKER_ICON,
                command=lambda marker, info=info_text: self.on_marker_click(info)
            )
            self.map_markers[(city, state)] = marker
            logging.info(f"[MapMarkers] Marker set for {city}, {state} at ({lat}, {lon})")
        except Exception as e:
            logging.error(f"[MapMarkers] Failed to set marker for {city}, {state}: {e}")

    def _geocode_locations_worker(self):
        """Background thread: geocode locations from the queue and store results in a thread-safe list."""
        from geopy.geocoders import Nominatim
        import time
        geolocator = Nominatim(user_agent=APP_NAME)
        self._geocoded_results = []
        while not self.geocoding_queue.empty():
            try:
                city, state = self.geocoding_queue.get_nowait()
            except Exception:
                break
            try:
                location = geolocator.geocode(f"{city}, {state}, USA", timeout=10)
                if location:
                    coords = (location.latitude, location.longitude)
                    add_cached_location_db(f"{city}|{state}", location.latitude, location.longitude)
                    self._geocoded_results.append((city, state, coords))
                    logging.info(f"[MapMarkers] Geocoded {city}, {state}: {coords}")
                else:
                    logging.warning(f"[MapMarkers] Geocode failed for {city}, {state}")
            except Exception as e:
                logging.warning(f"[MapMarkers] Geocode error for {city}, {state}: {e}")
            time.sleep(1.0)  # Be nice to Nominatim

    def _process_geocoding_results(self):
        """Process geocoded results from the background thread and place markers on the map."""
        if hasattr(self, '_geocoded_results') and self._geocoded_results:
            for city, state, coords in self._geocoded_results:
                self._place_map_marker(city, state, coords)
            self._geocoded_results.clear()
            if self.map_status_label:
                self.map_status_label.config(text=f"Map status: {len(self.map_markers)} locations loaded (with geocoding)")
        # Continue polling if thread is alive and queue not empty
        if hasattr(self, 'geocoding_thread') and self.geocoding_thread and self.geocoding_thread.is_alive():
            self._geocoding_after_id = self.root.after(500, self._process_geocoding_results)
        else:
            self.processing_queue = False
            if self.map_status_label:
                self.map_status_label.config(text=f"Map status: {len(self.map_markers)} locations loaded (all done)")
    # Removed duplicate import_cases_from_xlsx function - using the newer version below

    def on_closing(self):
        """Safely handle application shutdown, canceling background tasks and scheduled callbacks, and ensuring map widget is destroyed to prevent background errors."""
        # Mark shutting down to let background error handler ignore late callbacks
        try:
            self._is_shutting_down = True
        except Exception:
            pass
        # Cancel any scheduled .after() callbacks
        try:
            if hasattr(self, '_geocoding_after_id') and self._geocoding_after_id:
                try:
                    self.root.after_cancel(self._geocoding_after_id)
                    self._geocoding_after_id = None
                except Exception as e:
                    logging.warning(f"Error cancelling scheduled after callback: {e}")
        except Exception as e:
            logging.warning(f"Error during after_cancel cleanup: {e}")

        # Always destroy the map widget if it exists, to prevent background update errors
        try:
            if hasattr(self, 'map_widget') and self.map_widget is not None:
                try:
                    # tkintermapview provides a method to destroy/cleanup, if available
                    if hasattr(self.map_widget, 'destroy'):
                        self.map_widget.destroy()
                except Exception as e:
                    logging.warning(f"Error destroying map widget: {e}")
                finally:
                    self.map_widget = None
        except Exception as e:
            logging.warning(f"Error during map widget cleanup: {e}")

        # Optionally, set a flag to stop background threads (if you have a custom thread loop)
        self.processing_queue = False

        # Join geocoding thread with a short timeout to ensure clean shutdown
        try:
            if hasattr(self, 'geocoding_thread') and self.geocoding_thread is not None:
                # Clear queue to unblock worker quickly
                try:
                    while not self.geocoding_queue.empty():
                        self.geocoding_queue.get_nowait()
                except Exception:
                    pass
                # Wait briefly for thread to finish
                self.geocoding_thread.join(timeout=1.5)
        except Exception as e:
            logging.debug(f"Geocoding thread join skipped/failed: {e}")

        # Destroy matplotlib canvas to stop redraw callbacks
        try:
            if hasattr(self, 'canvas_agg') and self.canvas_agg is not None:
                try:
                    w = self.canvas_agg.get_tk_widget()
                    w.destroy()
                except Exception:
                    pass
                finally:
                    self.canvas_agg = None
        except Exception:
            pass

        # Attempt a database backup before closing
        try:
            perform_db_backup(retention_days=56)
        except Exception as e:
            logging.warning(f"Database backup on close failed: {e}")

        # Destroy the main window
        try:
            # Prevent further Tkinter events from being processed after destroy
            if hasattr(self, 'root'):
                try:
                    self.root.quit()
                except Exception:
                    pass
                # Delay actual destroy slightly to let quit unwind and pending callbacks be ignored
                try:
                    self.root.after(50, self.root.destroy)
                except Exception:
                    self.root.destroy()
            logging.info("Application shutting down.")
        except Exception as e:
            logging.error(f"Error during shutdown: {e}")
    def __init__(self, root):
        self.root = root
        self.root.title(APP_NAME)
        self.root.geometry("1250x850")
        # Use ttkbootstrap's style system if available, else fallback to ttk
        if hasattr(self.root, 'style'):
            self.style = self.root.style
            is_bootstrap = True
            default_theme = 'flatly'  # ttkbootstrap default
        else:
            self.style = ttk.Style(self.root)
            is_bootstrap = False
            default_theme = 'clam'  # Tkinter default

        # Load saved theme or use default, and ensure it is valid
        saved_theme = get_user_pref('theme', default_theme)
        available_themes = self.style.theme_names() if hasattr(self.style, 'theme_names') else []
        if saved_theme in available_themes:
            self.style.theme_use(saved_theme)
            self._saved_theme_code = saved_theme
        else:
            # Fallback to default, update user pref, and log warning
            self.style.theme_use(default_theme)
            set_user_pref('theme', default_theme)
            self._saved_theme_code = default_theme
            logging.warning(f"Saved theme '{saved_theme}' not available. Falling back to '{default_theme}'.")

        # Attributes for entry widgets
        self.entries = {} # Dictionary to hold Tkinter variables/widgets for form fields
        self.editing_case_id = None # Variable to track if we are currently editing a case (None or case_id)
        self.submit_button = None # Reference to the submit button for text changes
        self.field_frame_container = None # Reference to the frame holding input fields

        # Attributes for logo image
        self.logo_path = tk.StringVar(value=LOGO_FILENAME) # Track the path, though we primarily use the loaded image
        self.logo_image_tk = None # Image for display in the Entry tab (scaled)
        self.logo_image_tk_preview = None # Separate image for the settings preview (thumbnail)
        self.entry_logo_label = None # Attribute to store the logo label in the Entry tab (needed to update its image)
        self.logo_preview_canvas = None # Reference to the settings logo preview canvas

        # Attributes for marker icon images
        self.marker_icon_tk_map = None # Image for map markers (20x20)
        self.marker_icon_tk_preview = None # Image for settings preview (e.g., 50x50)
        self.marker_icon_preview_canvas = None # Reference to the settings preview canvas

    # Defer initial logo load until after widgets are created so labels exist
        self.load_marker_icon_image() # Load the marker icon upon app initialization


        # Attributes for Map View
        self.map_widget = None
        self.map_status_label = None
        # Geopy geolocator instance - only create one per thread. Not needed in main thread.
        # self.geolocator = Nominatim(user_agent=APP_NAME)
        self.map_markers = {} # Dictionary to hold mapview markers with location (city, state) as key
        self._grouped_cases_by_location = {} # Store cases grouped by location for info bubbles


        # Attributes for View Data Treeview
        self.tree = None
        self.tree_columns_config = {} # Dictionary to store treeview column configuration
        self.treeview_sort_column = None # To keep track of the currently sorted column
        self.treeview_sort_reverse = False # To keep track of the sort order

        # Attributes for Graph Tab
        self.fig = None # Matplotlib figure
        self.ax = None # Matplotlib axes
        self.canvas_agg = None # FigureCanvasTkAgg

        # Attributes for Status Bar
        self.status_label = None
        self.status_animation_id = None
        self.status_text = ""


        # Attributes for threading and queue for map loading
        self.geocoding_queue = queue.Queue()
        self.geocoding_thread = None
        self.processing_queue = False # Flag to indicate if we are currently checking the queue
        self.geolocated_count = 0 # Initialize count for geolocated markers (locations)
        self.skipped_count = 0 # Initialize count for skipped locations
        self._geocoding_after_id = None # ID for the scheduled _process_geocoding_results after call


        # Always ensure DB is initialized before any data access
        try:
            init_db()
        except Exception as e:
            logging.error(f"Failed to initialize database at startup: {e}")
            Messagebox.show_error("Database Error", f"Failed to initialize database: {e}")
            # Optionally, exit or disable UI
        self.create_widgets() # Create all the main UI widgets
        # Now that widgets exist, ensure logo is loaded and displayed if present
        try:
            if os.path.exists(LOGO_FILENAME):
                self.logo_path.set(LOGO_FILENAME)
            self.load_logo_image()
        except Exception:
            pass

        # Status Bar creation (Moved here to ensure self.status_label exists before status updates)
        self.status_label = ttk.Label(self.root, text="Initializing...", anchor='w')
        self.status_label.grid(row=1, column=0, sticky='ew', padx=10, pady=(0, 5))
        self.update_status("Initializing...")

        # Perform initial data loading and UI refresh
        self.refresh_data_view() # Populate the treeview
        self.load_map_markers() # This now starts the threaded geocoding
        self.populate_graph_filters() # Populate filters for the graph
        self.update_graph() # Display initial graph

        # Initial status is set by the map loading process or defaults below if map loading is skipped
        # The _finalize_map_loading will set the final status
        # Ensure status is cleared if thread finishes quickly
        if not self.geocoding_thread or not self.geocoding_thread.is_alive():
            self.update_status("Ready")

        # Set the window closing protocol to call the cleanup function
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        # Install a Tkinter background error handler to suppress benign errors during shutdown
        self._is_shutting_down = False
        def _tk_error_handler(exc, val, tb):
            msg = str(val)
            if self._is_shutting_down and (
                'application has been destroyed' in msg.lower() or
                'invalid command name' in msg.lower() or
                'update_canvas_tile_images' in msg
            ):
                # Ignore late callbacks fired after shutdown began
                logging.debug(f"Ignored Tk callback error during shutdown: {msg}")
                return
            # Log other exceptions normally
            logging.error("Tkinter callback error", exc_info=(exc, val, tb))
        try:
            self.root.report_callback_exception = _tk_error_handler
        except Exception:
            pass
        # Optional: silent update check shortly after startup (notify-only)
        try:
            self.root.after(2500, lambda: self.check_for_updates(silent=True))
        except Exception:
            pass
    def create_widgets(self):
        """
        Creates the main notebook tabs and calls methods to populate them.
        """
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)
        self.notebook = tb.Notebook(self.root)
        self.notebook.grid(row=0, column=0, sticky='nsew', padx=10, pady=10)

        # Progress bar for long operations (initially hidden) - placed in main window
        self.progress = ttk.Progressbar(self.root, orient="horizontal", mode="indeterminate")
        self.progress.grid(row=2, column=0, sticky='ew', padx=10, pady=(0,5))
        self.progress.grid_remove()  # Hide initially

        # Tabs (Dashboard removed)
        self.entry_frame = tb.Frame(self.notebook, padding="10")
        self.view_frame = tb.Frame(self.notebook, padding="10")
        self.map_frame = tb.Frame(self.notebook, padding="10")
        self.graph_frame = tb.Frame(self.notebook, padding="10")
        self.settings_frame = tb.Frame(self.notebook, padding="10")
        self.about_frame = tb.Frame(self.notebook, padding="10")

        for frame in [self.entry_frame, self.view_frame, self.map_frame, self.graph_frame, self.settings_frame, self.about_frame]:
            frame.rowconfigure(0, weight=1)
            frame.columnconfigure(0, weight=1)

        self.notebook.add(self.entry_frame, text='New Case Entry')
        self.notebook.add(self.view_frame, text='View Data')
        self.notebook.add(self.map_frame, text='Map View')
        self.notebook.add(self.graph_frame, text='Graphs')
        self.notebook.add(self.settings_frame, text='Settings')
        self.notebook.add(self.about_frame, text='About')

        self.create_entry_widgets()
        self.create_view_widgets()
        self.create_map_widgets()
        self.create_graph_widgets()
        self.create_settings_widgets()
        self.init_lazy_loading()

        # About tab content
        about_text = (
            "CyberLab Case Tracker\n\n"
            "A desktop tool for digital forensics labs to log cases, generate reports, map activity, and analyze workload.\n\n"
            "Key Features:\n"
            "- New Case Entry with editable dropdowns and smart defaults\n"
            "- View Data: fast search/filter, lazy loading, edit/delete with undo/redo, customizable columns\n"
            "- Reports: PDF/XLSX (Date Range Summary, Case Summary, Custom, All Cases); saved path shown in dialogs\n"
            "- Report header info (Name/Agency/Division/Date) persisted across sessions\n"
            "- Map View: markers by city/state with cached geocoding; optional state focal\n"
            "- Graphs: offense, device, agency, examiner, and more\n"
            "- Settings: themes, logo and map marker icon, column visibility\n"
            "- Security: password-protected sensitive actions; default 'password' on first run\n"
            "- Reliability: automatic database backup on close with 56-day retention\n"
            "- Updates: notify-only update check (About tab)\n\n"
            "How to Use (by tab):\n"
            "- New Case Entry: Fill out case details, use date pickers for dates, then Submit to save to the database.\n"
            "- View Data: Use the search box to filter. Click column headers to sort. Select a row to edit or delete. Use report actions to export Case Summary, Date Range, or Custom reports.\n"
            "- Map View: Optionally pick a state to focus. Click map markers to see offense types for that location. Geocoding results are cached.\n"
            "- Graphs: Choose a graph type and (optional) year filter to visualize trends across cases.\n"
            "- Settings: Change theme, set report header (Name/Agency/Division), choose logo/marker icons, and control visible columns in View Data.\n"
            "- About: View version and data locations, and run a check for updates.\n\n"
            "Data & Paths:\n"
            f"- Data Dir: {DATA_DIR}\n"
            f"- Database: {DB_FILENAME}\n"
            f"- Backups: {BACKUP_DIR}\n"
            f"- Log File: {LOG_FILENAME}\n\n"
            f"Version: {APP_VERSION}\n"
        )

        from tkinter import scrolledtext
        about_box = scrolledtext.ScrolledText(self.about_frame, wrap='word', font=("Segoe UI", 11), state='normal', height=28, width=100)
        about_box.insert('1.0', about_text)
        about_box.config(state='disabled')
        about_box.grid(row=0, column=0, sticky='nsew', padx=10, pady=10)

        # Add clickable GitHub link at the bottom
        github_url = "https://github.com/RF-YVY"
        def open_github(event=None):
            import webbrowser
            webbrowser.open_new(github_url)

        github_label = tk.Label(self.about_frame, text="Visit Developer GitHub: https://github.com/RF-YVY", fg="blue", cursor="hand2", font=("Segoe UI", 10, "underline"))
        github_label.grid(row=1, column=0, sticky='w', padx=18, pady=(0, 12))
        github_label.bind("<Button-1>", open_github)

        # Add Check for Updates button on About tab
        updates_frame = ttk.Frame(self.about_frame)
        updates_frame.grid(row=2, column=0, sticky='w', padx=10, pady=(0, 10))
        ttk.Button(updates_frame, text=f"Check for Updates (v{APP_VERSION})", command=lambda: self.check_for_updates(silent=False)).pack(side='left')

    # Dashboard tab and window have been removed as requested.

    def create_entry_widgets(self):
        """Creates the widgets for the New Case Entry tab."""
        # Create a main frame that will hold all content for the entry tab
        entry_content_frame = tb.Frame(self.entry_frame)
        entry_content_frame.grid(row=0, column=0, sticky='nsew')
        self.entry_frame.rowconfigure(0, weight=1)
        self.entry_frame.columnconfigure(0, weight=1)

        # Create a Canvas and Scrollbar for the scrollable area
        canvas = tk.Canvas(entry_content_frame)
        scrollbar = ttk.Scrollbar(entry_content_frame, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.grid(row=0, column=0, sticky='nsew')
        scrollbar.grid(row=0, column=1, sticky='ns')
        entry_content_frame.rowconfigure(0, weight=1)
        entry_content_frame.columnconfigure(0, weight=1)

        # Create the frame that will be inside the canvas and hold all your scrollable widgets
        scrollable_frame = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion = canvas.bbox("all")))

        # --- Now, place all your subsequent widgets inside scrollable_frame ---


        # Top section: Title and Logo
        top_section_frame = tb.Frame(scrollable_frame) # Parent is scrollable_frame
        top_section_frame.pack(fill='x', pady=10, padx=10) # Pack within scrollable_frame

        title_label = tb.Label(top_section_frame, text="New Case Entry", font=("Arial", 16, "bold"))
        title_label.pack(side='left', anchor='nw')

        # Place an overlay logo label in the top-right of the Entry tab (does not affect field layout)
        self.entry_logo_label = ttk.Label(self.entry_frame, text="")
        # Anchor to top-right with a small margin so it doesn't cover the scrollbar
        self.entry_logo_label.place(relx=1.0, rely=0.0, x=-20, y=10, anchor='ne')
        # Ensure it stays above other widgets
        try:
            self.entry_logo_label.lift()
        except Exception:
            pass
        # Update the image now that the label exists
        try:
            self.load_logo_image()
        except Exception:
            pass


        self.entries = {} # Dictionary to hold Tkinter variables/widgets for form fields
        # Frame to hold the grid of input fields
        self.field_frame_container = ttk.Frame(scrollable_frame) # Parent is scrollable_frame, store reference
        # Pack the field_frame_container below the top_section_frame within the scrollable_frame
        self.field_frame_container.pack(fill='both', expand=True, anchor='nw', pady=(10,0), padx=10)
        self.field_frame_container.rowconfigure(0, weight=1)
        self.field_frame_container.columnconfigure(0, weight=1)
        self.field_frame_container.columnconfigure(1, weight=1)
        self.field_frame_container.grid_columnconfigure(0, weight=1)
        self.field_frame_container.grid_columnconfigure(1, weight=1)
        # Define the fields to be created: (Label Text, Dictionary Key, Widget Type, Options)
        # Widget Type: "entry", "combo", "check", "date", "text"

        fields_definition = [
            ("Examiner", "examiner", "combo", []),           # Changed to combo
            ("Investigator", "investigator", "combo", []),   # Changed to combo
            ("Agency", "agency", "combo", []),               # Changed to combo
            ("Cyber Case #", "case_number", "entry"),
            ("Volume Size (GB)", "volume_size_gb", "entry"),
            ("Type of Offense", "offense_type", "combo", []),# Changed to combo
            ("City of Offense", "city_of_offense", "combo", []), 
            ("State of Offense", "state_of_offense", "combo", US_STATE_ABBREVIATIONS), # Added State here
            ("Device Type", "device_type", "combo", ["", "iOS", "Android", "ChromeOS", "Windows", "SD", "HDD", "SDD", "USB", "SW Return", "Zip file", "drone", "other"]),
            ("Model", "model", "entry"),
            ("OS", "os", "entry")
        ]

        current_row = 0 # Initialize row counter for grid layout
        for i, (label_text, key, field_type, *options) in enumerate(fields_definition):
            row, col = divmod(i, 2) # Arrange fields in two columns
            current_row = row # Keep track of the current row used by the loop

            cell_frame = ttk.Frame(self.field_frame_container, padding=(0,0,10,5)) # Parent is field_frame_container
            cell_frame.grid(row=row, column=col, sticky='ew', padx=5, pady=2)
            self.field_frame_container.grid_columnconfigure(col, weight=1) # Make columns expandable

            label = ttk.Label(cell_frame, text=label_text)
            label.pack(side='top', anchor='w')

            if field_type == "entry":
                entry = tb.Entry(cell_frame, width=40)
                entry.pack(side='top', fill='x', expand=True)
                self.entries[key] = entry
            elif field_type == "combo":
                var = tk.StringVar()
                # Load persistent values for editable combos
                if key in ["examiner", "investigator", "agency", "offense_type", "city_of_offense"]:
                    combo_values = get_combo_values_db(key)
                else:
                    combo_values = options[0] if options and options[0] else []
                combo = ttk.Combobox(cell_frame, textvariable=var, values=combo_values, state="normal", width=38)
                combo.pack(side='top', fill='x', expand=True)

                # Set default for State of Offense
                if key == "state_of_offense":
                    if "MS" in combo_values:
                        var.set("MS")
                    elif combo_values:
                        var.set(combo_values[0])
                elif combo_values:
                    var.set(combo_values[0])

                # --- Add dynamic entry logic for editable combos ---
                if key in ["examiner", "investigator", "agency", "offense_type", "city_of_offense"]:
                    def add_to_combo(event, combo=combo, var=var, key=key):
                        value = var.get().strip()
                        values = list(combo['values'])
                        if value and value not in values:
                            values.append(value)
                            combo['values'] = values
                            set_combo_values_db(key, values)  # Persist new value
                    combo.bind("<Return>", add_to_combo)
                    combo.bind("<FocusOut>", add_to_combo)

                self.entries[key] = var

        # --- Data Recovered? and FPR Complete? on the same row ---
        # Place checkboxes after the last row of the grid, but before notes
        # Make sure the checkboxes are visible by using pack instead of grid for the containing frame

        check_row = current_row + 1
        check_frame = ttk.Frame(self.field_frame_container, padding=(0,0,10,5))
        check_frame.grid(row=check_row, column=0, columnspan=2, sticky='ew', padx=5, pady=2)
        self.field_frame_container.grid_rowconfigure(check_row, weight=0)

        # Use an internal frame with pack to ensure visibility
        checks_inner = ttk.Frame(check_frame)
        checks_inner.pack(fill='x', expand=True)

        checks_label = ttk.Label(checks_inner, text="Case Status:", font=("Arial", 10, "bold"))
        checks_label.pack(side='left', anchor='w', padx=(0, 10))

        dr_var = tk.BooleanVar()
        dr_chk = tb.Checkbutton(checks_inner, variable=dr_var, text="Data Recovered ?")
        dr_chk.pack(side='left', anchor='w', padx=(0, 30))
        self.entries['data_recovered'] = dr_var

        fpr_var = tk.BooleanVar()
        fpr_chk = tb.Checkbutton(checks_inner, variable=fpr_var, text="FPR Complete ?")
        fpr_chk.pack(side='left', anchor='w', padx=(0, 0))
        self.entries['fpr_complete'] = fpr_var


        # --- Notes field ---
        # Place notes field after the checkboxes row
        notes_row = check_row + 1  # FIX: Place notes below checkboxes, not at same row
        notes_frame = tb.LabelFrame(self.field_frame_container, text="Notes", padding="5") # Parent is field_frame_container
        notes_frame.grid(row=notes_row, column=0, columnspan=2, sticky='ewns', padx=5, pady=(10,5))
        self.field_frame_container.grid_rowconfigure(notes_row, weight=1) # Allow notes field to expand vertically

        txt_notes = tk.Text(notes_frame, height=6, width=40, wrap='word')
        txt_notes_scroll = tb.Scrollbar(notes_frame, orient='vertical', command=txt_notes.yview)
        txt_notes['yscrollcommand'] = txt_notes_scroll.set

        txt_notes_scroll.pack(side='right', fill='y')
        txt_notes.pack(side='left', fill='both', expand=True)

        self.entries['notes'] = txt_notes # Store the Text widget reference

        # --- DateEntry Fields ---
        # This block must come AFTER the Notes field block (where notes_row is defined)
        date_row = notes_row + 1 # Calculate the row for DateEntry fields based on the Notes field's row

        date_field_info = [("Start Date (MM-DD-YYYY)", "start_date"), ("End Date (MM-DD-YYYY)", "end_date")]
        for i, (label_text, key) in enumerate(date_field_info):
            col = i # Dates will be side-by-side (column 0 and 1)
            cell_frame = ttk.Frame(self.field_frame_container, padding=(0,0,10,5)) # Parent is field_frame_container
            cell_frame.grid(row=date_row, column=col, sticky='ew', padx=5, pady=2) # Use date_row

            label = ttk.Label(cell_frame, text=label_text)
            label.pack(side='top', anchor='w')

            date_entry = tb.DateEntry(cell_frame, width=36, dateformat='%m-%d-%Y')
            date_entry.pack(side='left', fill='x', expand=True)

            self.entries[key] = date_entry


        # --- Submit and Cancel Buttons ---
        # This frame should be placed after the date fields. Determine the row after date fields.
        # Assuming DateEntry fields are on one row (date_row), the buttons go on the next row.
        submit_button_row = date_row + 1

        submit_button_frame = ttk.Frame(scrollable_frame) # Parent is scrollable_frame
        submit_button_frame.pack(fill='x', pady=(15, 10), anchor='w', padx=10)

        # Submit button (store reference)
        self.submit_button = tb.Button(submit_button_frame, text="Submit Case", command=self.submit_case, style="Accent.TButton")
        self.submit_button.pack(side='left') # Pack left

        # Add a Cancel Edit/Clear Form button
        cancel_button = ttk.Button(submit_button_frame, text="Clear Form", command=self.clear_entry_form)
        cancel_button.pack(side='left', padx=(5,0)) # Pack next to submit button

        # Configure Accent button style (defined here as used in this tab)
        self.style.configure("Accent.TButton", font=("-weight", "bold"))
        
        # Configure Danger button style (for delete buttons)
        self.style.configure("Danger.TButton", foreground="red", font=("-weight", "bold"))


        # After all widgets are created in create_entry_widgets
        for key in ["examiner", "investigator", "agency", "offense_type", "city_of_offense"]:
            if key in self.entries and isinstance(self.entries[key], tk.StringVar):
                combo_widget = None
                for child in self.field_frame_container.winfo_children():
                    for grandchild in child.winfo_children():
                        if isinstance(grandchild, ttk.Combobox) and grandchild.cget('textvariable') == str(self.entries[key]):
                            combo_widget = grandchild
                            break
                    if combo_widget:
                        break
                if combo_widget:
                    combo_widget['values'] = get_unique_field_values(key)

        # Auto-populate Examiner with last used value
        last_examiner = self.get_last_examiner()
        if last_examiner and 'examiner' in self.entries and isinstance(self.entries['examiner'], tk.StringVar):
            self.entries['examiner'].set(last_examiner)

        # --- Auto-populate State of Offense with last used value (persistent) ---
        last_state = self.get_last_state_of_offense()
        if last_state and 'state_of_offense' in self.entries and isinstance(self.entries['state_of_offense'], tk.StringVar):
            self.entries['state_of_offense'].set(last_state)

        # Save new state when changed
        if 'state_of_offense' in self.entries and isinstance(self.entries['state_of_offense'], tk.StringVar):
            def on_state_change_var(*args):
                state = self.entries['state_of_offense'].get()
                if state:
                    self.set_last_state_of_offense(state)
            self.entries['state_of_offense'].trace_add('write', on_state_change_var)

    def get_last_state_of_offense(self):
        """Return the last used state of offense from user prefs, or from the most recent case if not set."""
        state = get_user_pref('last_state_of_offense', None)
        if state:
            return state
        # Fallback: get from most recent case
        try:
            cases = get_all_cases_db()
            for case in reversed(cases):
                s = case.get('state_of_offense')
                if s:
                    return s
        except Exception as e:
            logging.warning(f"Could not get last state of offense: {e}")
        return None

    def set_last_state_of_offense(self, state):
        """Persist the last used state of offense to user prefs."""
        set_user_pref('last_state_of_offense', state)

    def get_last_examiner(self):
        """Return the last used examiner from the most recent case, or None if not found."""
        try:
            cases = get_all_cases_db()
            if cases:
                # Find the most recent case with a non-empty examiner
                for case in reversed(cases):
                    examiner = (case.get('examiner') or '').strip()
                    if examiner:
                        return examiner
        except Exception as e:
            logging.warning(f"Could not get last examiner: {e}")
        return None


    def create_view_widgets(self):
        """Creates the widgets for the View Data tab (Treeview, buttons) and adds a search/filter bar."""
        self.init_view_edit_history()
        self.init_lazy_loading()

        # Container for the View Data layout
        container = ttk.Frame(self.view_frame)
        container.grid(row=0, column=0, sticky='nsew')
        self.view_frame.rowconfigure(0, weight=1)
        self.view_frame.columnconfigure(0, weight=1)

        # --- Search/Filter Bar ---
        search_frame = ttk.Frame(container)
        search_frame.grid(row=0, column=0, sticky='ew', pady=(5, 0), padx=5)
        ttk.Label(search_frame, text="Search/Filter:").pack(side='left', padx=(0, 5))
        self.view_search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.view_search_var, width=40)
        search_entry.pack(side='left', padx=(0, 5))
        search_button = ttk.Button(search_frame, text="Apply", command=self.apply_view_filter)
        search_button.pack(side='left')
        clear_button = ttk.Button(search_frame, text="Clear", command=self.clear_view_filter)
        clear_button.pack(side='left', padx=(5, 0))
        search_entry.bind('<Return>', lambda e: self.apply_view_filter())

        # Reports dropdown consolidating Date Range, Case Summary, Custom Report, Export All (PDF/XLSX)
        reports_mb = tk.Menubutton(search_frame, text="Reports", relief="raised")
        reports_menu = tk.Menu(reports_mb, tearoff=0)
        reports_menu.add_command(label="Date Range...", command=self.show_total_case_summary)
        reports_menu.add_command(label="Case Summary (selected)...", command=self.show_case_summary_report)
        reports_menu.add_command(label="Custom Report...", command=self.show_custom_report_builder)
        reports_menu.add_command(label="All Cases (Summary)...", command=self.show_all_cases_summary)
        reports_menu.add_separator()
        reports_menu.add_command(label="Export All as PDF", command=self.export_pdf_report)
        reports_menu.add_command(label="Export All as XLSX", command=self.export_xlsx_report)
        reports_mb.configure(menu=reports_menu)
        reports_mb.pack(side='left', padx=(10, 0))

        # Columns selection
        columns_button = ttk.Button(search_frame, text="Columns", command=self.show_column_selector)
        columns_button.pack(side='left', padx=(10, 0))

        # Button frame for Refresh, Export, Edit, Delete, Undo, Redo
        button_frame = ttk.Frame(container)
        button_frame.grid(row=1, column=0, sticky='ew', pady=(0, 10), padx=5)

        refresh_button = ttk.Button(button_frame, text="Refresh Data", command=self.refresh_data_view)
        refresh_button.pack(side='left', padx=(0, 5))

        # Add Edit Selected button
        edit_button = ttk.Button(button_frame, text="Edit Selected", command=self.edit_selected_case)
        edit_button.pack(side='left', padx=(0, 5))

        # Add a Delete Selected button
        delete_button = ttk.Button(button_frame, text="Delete Selected", command=self.delete_selected_cases, style="Danger.TButton")
        delete_button.pack(side='left')

        # Add Undo/Redo buttons
        undo_button = ttk.Button(button_frame, text="Undo Edit", command=self.undo_view_edit)
        undo_button.pack(side='left', padx=(10, 2))
        redo_button = ttk.Button(button_frame, text="Redo Edit", command=self.redo_view_edit)
        redo_button.pack(side='left', padx=(2, 0))

        # Frame to hold the Treeview and its scrollbars
        tree_frame = ttk.Frame(container)
        tree_frame.grid(row=2, column=0, sticky='nsew', padx=5, pady=5)
        container.rowconfigure(2, weight=1)
        container.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(tree_frame, show='headings')

        # Store the database column names along with display text and other config
        # Ensure 'id' is included but marked as not visible
        self.tree_columns_config = {
            "id": {"text": "ID", "width": 0, "visible": False},
            "case_number": {"text": "Case #", "width": 100},
            "examiner": {"text": "Examiner", "width": 100},
            "investigator": {"text": "Investigator", "width": 100},
            "agency": {"text": "Agency", "width": 100},
            "city_of_offense": {"text": "City", "width": 100},
            "state_of_offense": {"text": "State", "width": 80},
            "start_date": {"text": "Start (MM-DD-YYYY)", "width": 100, "type": "date"},
            "end_date": {"text": "End (MM-DD-YYYY)", "width": 100, "type": "date"},
            "volume_size_gb": {"text": "Vol (GB)", "width": 60, "type": "numeric"},
            "offense_type": {"text": "Offense", "width": 120},
            "device_type": {"text": "Device", "width": 100},
            "model": {"text": "Model", "width": 100},
            "os": {"text": "OS", "width": 80},
            "data_recovered": {"text": "Recovered?", "width": 70},
            "fpr_complete": {"text": "FPR?", "width": 50, "type": "boolean"},
            "created_at": {"text": "Created (MM-DD-YYYY)", "width": 100, "type": "date"},
            "notes": {"text": "Notes", "width": 200}
        }

        # Use all keys from config as internal treeview columns
        self.tree["columns"] = list(self.tree_columns_config.keys())
        # Use only user-selected columns for Treeview display columns
        visible_columns = self.get_visible_treeview_columns()
        self.tree.configure(displaycolumns=visible_columns)

        for col_key, config in self.tree_columns_config.items():
            self.tree.column(col_key, anchor='w', width=config["width"], stretch=tk.YES)
            if col_key in visible_columns:
                self.tree.heading(col_key, text=config["text"], command=lambda c=col_key: self.sort_treeview_column(c))
            self.tree.column(col_key, anchor='w', width=config["width"], stretch=tk.NO)
            if not config.get("visible", True):
                self.tree.column(col_key, width=0, stretch=tk.NO)

        # Scrollbars for the Treeview
        vsb = ttk.Scrollbar(tree_frame, orient="vertical")
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        # Bind vertical scrollbar to lazy loading
        def on_vsb(*args):
            self.on_treeview_scroll(*args)
            vsb.set(*args)
        vsb.config(command=on_vsb)
        self.tree.bind("<MouseWheel>", lambda e: self.on_treeview_scroll("scroll", int(-1*(e.delta/120)), "units"))

        self.tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        # --- Accessibility: Keyboard navigation, focus, labeling ---
        self.tree.bind('<Return>', lambda e: self.edit_selected_case())
        self.tree.bind('<Delete>', lambda e: self.delete_selected_cases())
        self.tree.bind('<Control-c>', lambda e: self.copy_selected_treeview_rows())
        self.tree.bind('<Tab>', lambda e: self.focus_next_widget(e))
        self.tree.bind('<Shift-Tab>', lambda e: self.focus_prev_widget(e))
        self.tree.bind('<FocusIn>', lambda e: self.update_status("Treeview focused. Use arrows, Enter to edit, Del to delete, right-click for menu."))
        self.tree.bind('<Button-3>', self.on_treeview_right_click)
        self.tree.bind('<Menu>', self.on_treeview_right_click)
        self.tree['takefocus'] = True

        # Set accessible headings
        for col_key, config in self.tree_columns_config.items():
            self.tree.heading(col_key, text=config["text"], command=lambda c=col_key: self.sort_treeview_column(c))

        # Set accessible names for buttons and inputs (guard if some are missing)
        for name in ['search_button', 'clear_button', 'refresh_button', 'edit_button', 'delete_button', 'undo_button', 'redo_button', 'columns_button', 'reports_mb']:
            btn = locals().get(name)
            if btn is not None:
                try:
                    btn['takefocus'] = True
                    btn['cursor'] = 'hand2'
                except Exception:
                    pass
        se = locals().get('search_entry')
        if se is not None:
            try:
                se['takefocus'] = True
                se['cursor'] = 'xterm'
            except Exception:
                pass
        # 'aria-label' is not a valid Tkinter option; skip for compatibility
        # (Optional) Add tooltips if a tooltip library is available

    # --- Context Menu for Treeview ---
    def on_treeview_right_click(self, event):
        # Show context menu on right-click or Menu key
        iid = self.tree.identify_row(event.y)
        if iid:
            self.tree.selection_set(iid)
        menu = tk.Menu(self.tree, tearoff=0)
        menu.add_command(label="Edit Selected", command=self.edit_selected_case, accelerator="Enter")
        menu.add_command(label="Delete Selected", command=self.delete_selected_cases, accelerator="Del")
        menu.add_command(label="Copy Selected", command=self.copy_selected_treeview_rows, accelerator="Ctrl+C")
        menu.add_separator()
        menu.add_command(label="Export Selected as PDF", command=self.export_selected_pdf)
        menu.add_command(label="Export Selected as XLSX", command=self.export_selected_xlsx)
        menu.tk_popup(event.x_root, event.y_root)

    def copy_selected_treeview_rows(self):
        # Copy selected rows to clipboard as tab-separated text
        selected = self.tree.selection()
        if not selected:
            self.update_status("No rows selected to copy.")
            return
        columns = self.tree['displaycolumns']
        rows = []
        for iid in selected:
            values = self.tree.item(iid, 'values')
            rows.append('\t'.join(str(values[self.tree['columns'].index(col)]) for col in columns))
        text = '\n'.join(rows)
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self.update_status(f"Copied {len(rows)} row(s) to clipboard.")

    def export_selected_pdf(self):
        # Export selected rows as PDF (reuse custom report logic)
        selected = self.tree.selection()
        if not selected:
            self.update_status("No rows selected to export.")
            return
        columns = self.tree['displaycolumns']
        headers = [self.tree_columns_config[c]['text'] for c in columns]
        rows = [self.tree.item(iid, 'values') for iid in selected]
        self.export_custom_report_pdf(headers, rows)

    def export_selected_xlsx(self):
        # Export selected rows as XLSX (reuse custom report logic)
        selected = self.tree.selection()
        if not selected:
            self.update_status("No rows selected to export.")
            return
        columns = self.tree['displaycolumns']
        headers = [self.tree_columns_config[c]['text'] for c in columns]
        rows = [self.tree.item(iid, 'values') for iid in selected]
        self.export_custom_report_xlsx(headers, rows)

    def focus_next_widget(self, event):
        event.widget.tk_focusNext().focus()
        return "break"

    def focus_prev_widget(self, event):
        event.widget.tk_focusPrev().focus()
        return "break"

    def create_map_widgets(self):
        """Creates the widgets for the Map View tab, including map view selection."""
        self.map_frame.rowconfigure(0, weight=1)
        self.map_frame.columnconfigure(0, weight=1)
        container = ttk.Frame(self.map_frame)
        container.grid(row=0, column=0, sticky='nsew')

        # --- State Focal Combo ---
        state_frame = ttk.Frame(container)
        state_frame.pack(fill='x', pady=(0, 5), padx=10)
        ttk.Label(state_frame, text="Focal State:").pack(side='left', padx=(0, 5))
        self.map_focal_state_var = tk.StringVar(value=self.get_map_focal_state())
        state_combo = ttk.Combobox(state_frame, textvariable=self.map_focal_state_var, values=US_STATE_ABBREVIATIONS, width=8, state='readonly')
        state_combo.pack(side='left')
        def on_state_change(event=None):
            state = self.map_focal_state_var.get()
            self.set_map_focal_state(state)
            self.focus_map_on_state(state)
        state_combo.bind('<<ComboboxSelected>>', on_state_change)
        # If a state is already set, focus map on it after widget creation
        self.map_frame.after(500, lambda: self.focus_map_on_state(self.map_focal_state_var.get()))

        # --- Map View Selection (Only free, public map tile servers) ---
        map_view_options = [
            ("Standard (OpenStreetMap)", "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png"),
            ("CartoDB Positron", "https://cartodb-basemaps-a.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png"),
            ("CartoDB Dark Matter", "https://cartodb-basemaps-a.global.ssl.fastly.net/dark_all/{z}/{x}/{y}.png"),
            # Stamen tiles now require an API key or are rate-limited; remove them for reliability
        ]

        map_view_names = [name for name, url in map_view_options if url]
        map_view_urls = {name: url for name, url in map_view_options if url}
        map_view_urls_rev = {url: name for name, url in map_view_options if url}

        # All these servers use XYZ scheme
        def is_maptiler_tms(url):
            return False

        map_view_frame = ttk.Frame(container)
        map_view_frame.pack(fill='x', pady=(0, 5), padx=10)
        ttk.Label(map_view_frame, text="Map View:").pack(side='left', padx=(0, 5))
        self.map_view_var = tk.StringVar()

        # Load saved map view or default
        saved_map_view = get_user_pref('map_view', map_view_options[0][1])
        if saved_map_view in map_view_urls_rev:
            self.map_view_var.set(map_view_urls_rev[saved_map_view])
        else:
            self.map_view_var.set(map_view_names[0])

        map_view_combo = ttk.Combobox(map_view_frame, textvariable=self.map_view_var, values=map_view_names, state="readonly", width=24)
        map_view_combo.pack(side='left')

        # No API key or MapTiler check needed

        # Map widget
        self.map_widget = tkintermapview.TkinterMapView(container, width=800, height=600, corner_radius=0)
        self.map_widget.pack(fill='both', expand=True)

        # Set initial position and zoom to Mississippi (or focal state if set)
        focal_state = self.map_focal_state_var.get()
        if focal_state:
            self.focus_map_on_state(focal_state)
        else:
            self.map_widget.set_position(32.7, -89.5)
            self.map_widget.set_zoom(7)

        # Set initial map view type, with TMS flag if needed
        try:
            tms_flag = is_maptiler_tms(saved_map_view)
            self.map_widget.tile_server_tms = tms_flag
            self.map_widget.set_tile_server(saved_map_view)
        except Exception:
            self.map_widget.tile_server_tms = False
            self.map_widget.set_tile_server(map_view_options[0][1])

        def on_map_view_change(event=None):
            selected_name = self.map_view_var.get()
            selected_url = map_view_urls[selected_name]
            try:
                tms_flag = is_maptiler_tms(selected_url)
                self.map_widget.tile_server_tms = tms_flag
                self.map_widget.set_tile_server(selected_url)
            except Exception:
                self.map_widget.tile_server_tms = False
                self.map_widget.set_tile_server(map_view_options[0][1])
            set_user_pref('map_view', selected_url)

        map_view_combo.bind("<<ComboboxSelected>>", on_map_view_change)

        # Status label for map loading
        self.map_status_label = ttk.Label(container, text="", anchor='w')
        self.map_status_label.pack(side='bottom', fill='x', pady=(5, 0))

        # Immediately load map markers after widget is created
        self.load_map_markers()
        if not self.map_markers:
            self.map_status_label.config(text="Map status: 0 locations loaded")

    def on_marker_click(self, info_text):
        """Display marker information in a formatted messagebox."""
        try:
            # Split the info text into city/state and offenses
            city_state, offenses = info_text.split("\n", 2)  # Take first two parts
            location_line = city_state.strip()
        
            # Format the offense types for better readability
            offenses = offenses.replace("Offense Types:", "").strip()
            # Create bullet points for each offense type
            offense_list = []
            for offense in offenses.split(","):
                if offense.strip():
                    offense_list.append(f"• {offense.strip()}")
        
            # Join offense list with newlines
            formatted_text = "\n".join(offense_list) if offense_list else "No offenses recorded"
        
            # Show the formatted information
            self._show_message(
                'info',
                "Location Information",
                f"{location_line}\n\nOffense Types:\n{formatted_text}",
            )
        except Exception as e:
            logging.error(f"Error displaying marker info: {e}")
            # Even in case of error, show something meaningful
            self._show_message('info', "Location Information", info_text)

    def create_graph_widgets(self):
        """Creates widgets for the Graphs tab."""
        self.graph_frame.rowconfigure(0, weight=1)
        self.graph_frame.columnconfigure(0, weight=1)
        container = ttk.Frame(self.graph_frame)
        container.grid(row=0, column=0, sticky='nsew')

        # Controls frame for graph options
        controls_frame = ttk.Frame(container)
        controls_frame.pack(fill='x', pady=5, padx=10)

        ttk.Label(controls_frame, text="Graph Type:").pack(side='left', padx=(0, 5))
        self.graph_type_var = tk.StringVar(value="Offense Type")
        self.graph_type_combo = tb.Combobox(
            controls_frame,
            textvariable=self.graph_type_var,
            values=[
                "Offense Type", "Device Type", "OS", "Agency", "State of Offense",
                "Examiner", "Investigator", "Year", "City of Offense", "Total Volume (GB/TB)",
                "Total Volume by Examiner", "Total Volume by Investigator", "Total Volume by Agency", "Total Volume by Device Type"
            ],
            state="readonly"
        )
        self.graph_type_combo.pack(side='left', padx=(0, 10))
        self.graph_type_combo.bind("<<ComboboxSelected>>", lambda e: self.update_graph())

        ttk.Label(controls_frame, text="Filter by Year:").pack(side='left', padx=(0, 5))
        self.graph_year_var = tk.StringVar(value="All")
        self.graph_year_combo = ttk.Combobox(
            controls_frame,
            textvariable=self.graph_year_var,
            values=["All"],
            state="readonly",
            width=8
        )
        self.graph_year_combo.pack(side='left', padx=(0, 10))
        self.graph_year_combo.bind("<<ComboboxSelected>>", lambda e: self.update_graph())


        graph_frame = ttk.Frame(container)
        graph_frame.pack(fill='both', expand=True, padx=10, pady=10)

        self.fig, self.ax = plt.subplots(figsize=(10, 6))
        self.canvas_agg = FigureCanvasTkAgg(self.fig, master=graph_frame)
        canvas_widget = self.canvas_agg.get_tk_widget()
        canvas_widget.pack(fill='both', expand=True)

        # --- Improved: Ensure graph always fits the UI window, including on first display and tab switch ---
        def on_graph_frame_configure(event=None):
            # Get the current size of the frame
            width = graph_frame.winfo_width()
            height = graph_frame.winfo_height()
            # Avoid zero size on initial event
            if width < 10 or height < 10:
                return
            # Resize the canvas and figure
            canvas_widget.configure(width=width, height=height)
            self.fig.set_size_inches(max(width/96, 4), max(height/96, 3), forward=True)
            self.canvas_agg.draw_idle()

        graph_frame.bind('<Configure>', on_graph_frame_configure)
        # Force an initial resize after widgets are packed
        graph_frame.after(100, on_graph_frame_configure)

        # Also trigger resize when the tab is selected (fixes first-show issue)
        def on_tab_changed(event=None):
            # Only trigger if the graph tab is selected
            if hasattr(self, 'notebook'):
                current_tab = self.notebook.select()
                if self.notebook.tab(current_tab, 'text') == 'Graphs':
                    on_graph_frame_configure()

        if hasattr(self, 'notebook'):
            self.notebook.bind('<<NotebookTabChanged>>', on_tab_changed)

    def update_graph(self):
        """Update the graph display in the Graphs tab."""
        cases = get_all_cases_db()
        graph_type = self.graph_type_var.get()
        year_filter = self.graph_year_var.get()

        # Filter by year if selected
        if year_filter and year_filter != "All":
            cases = [
                case for case in cases
                if case.get("start_date", "").startswith(year_filter)
            ]

        # Handle total volume by groupings
        group_volume_types = {
            "Total Volume by Examiner": "examiner",
            "Total Volume by Investigator": "investigator",
            "Total Volume by Agency": "agency",
            "Total Volume by Device Type": "device_type"
        }
        if graph_type in group_volume_types:
            group_field = group_volume_types[graph_type]
            data = {}
            for case in cases:
                group_val = (case.get(group_field) or "Unknown").strip() or "Unknown"
                try:
                    vol = case.get("volume_size_gb")
                    if vol is not None and vol != "":
                        data[group_val] = data.get(group_val, 0.0) + float(vol)
                except Exception:
                    continue
            # Sort by total volume descending
            sorted_items = sorted(data.items(), key=lambda x: x[1], reverse=True)
            labels = [item[0] for item in sorted_items]
            values = [item[1] for item in sorted_items]
            # Decide unit (GB or TB)
            use_tb = any(v > 999 for v in values)
            if use_tb:
                values_tb = [v / 1024.0 for v in values]
                y_label = "Total Volume (TB)"
                display_values = [f"{v:.2f}" for v in values_tb]
                plot_values = values_tb
            else:
                y_label = "Total Volume (GB)"
                display_values = [f"{v:.2f}" for v in values]
                plot_values = values
            self.ax.clear()
            bars = self.ax.bar(labels, plot_values, color="#4a90e2", align='center')
            self.ax.set_xlabel(group_field.replace('_', ' ').title())
            self.ax.set_ylabel(y_label)
            self.ax.set_title(f"{graph_type}")
            self.ax.tick_params(axis='x', rotation=45)
            self.fig.autofmt_xdate(rotation=45)
            self.fig.subplots_adjust(bottom=0.25)
            # Annotate values on bars
            for bar, val in zip(bars, display_values):
                height = bar.get_height()
                self.ax.text(bar.get_x() + bar.get_width()/2, height, val, ha='center', va='bottom', fontsize=9)
            self.fig.tight_layout()
            self.canvas_agg.draw()
            return

        if graph_type == "Total Volume (GB/TB)":
            # Calculate total volume
            total_gb = 0.0
            for case in cases:
                try:
                    val = case.get("volume_size_gb")
                    if val is not None and val != "":
                        total_gb += float(val)
                except Exception:
                    continue
            # Decide unit
            if total_gb > 999:
                total_tb = total_gb / 1024.0
                display_value = f"{total_tb:.2f} TB"
                y_val = total_tb
                y_label = "Total Volume (TB)"
            else:
                display_value = f"{total_gb:.2f} GB"
                y_val = total_gb
                y_label = "Total Volume (GB)"

            # Plot a single bar
            self.ax.clear()
            self.ax.bar(["Total"], [y_val], color="#4a90e2", align='center')
            self.ax.set_xlabel("")
            self.ax.set_ylabel(y_label)
            self.ax.set_title("Total Volume of All Cases")
            # Annotate value on bar
            self.ax.text(0, y_val, display_value, ha='center', va='bottom', fontsize=14, fontweight='bold')
            self.fig.tight_layout()
            self.canvas_agg.draw()
            return

        # Map graph type to DB field
        graph_field_map = {
            "Offense Type": "offense_type",
            "Device Type": "device_type",
            "OS": "os",
            "Agency": "agency",
            "State of Offense": "state_of_offense",
            "Examiner": "examiner",
            "Investigator": "investigator",
            "Year": "start_date",
            "City of Offense": "city_of_offense"
        }
        field = graph_field_map.get(graph_type, "offense_type")

        # Prepare data
        data = {}
        if field == "start_date" or graph_type == "Year":
            for case in cases:
                date_str = case.get("start_date", "")
                year = date_str[:4] if date_str else "Unknown"
                data[year] = data.get(year, 0) + 1
            xlabel = "Year"
        else:
            for case in cases:
                value = case.get(field, "") or "Unknown"
                data[value] = data.get(value, 0) + 1
            xlabel = graph_type

        # Sort data for display (greatest to least)
        sorted_items = sorted(data.items(), key=lambda x: x[1], reverse=True)
        labels = [item[0] for item in sorted_items]
        values = [item[1] for item in sorted_items]

        # Clear and plot
        self.ax.clear()
        if not labels:
            self.ax.text(0.5, 0.5, "No data to display", ha='center', va='center', fontsize=16)
        else:
            bars = self.ax.bar(labels, values, color="#4a90e2", align='center')
            self.ax.set_xlabel(xlabel)
            self.ax.set_ylabel("Count")
            self.ax.set_title(f"{graph_type} Distribution")
            self.ax.tick_params(axis='x', rotation=45)
            # For better label spacing
            self.fig.autofmt_xdate(rotation=45)
            # Optionally, adjust bottom margin for long labels
            self.fig.subplots_adjust(bottom=0.25)

        self.fig.tight_layout()
        self.canvas_agg.draw()

    def create_settings_widgets(self):
        # """Creates the widgets for the Settings tab."""
        self.settings_frame.rowconfigure(0, weight=1)
        self.settings_frame.columnconfigure(0, weight=1)
        # Scrollable container for Settings content
        settings_container = ttk.Frame(self.settings_frame)
        settings_container.grid(row=0, column=0, sticky='nsew')
        settings_container.rowconfigure(0, weight=1)
        settings_container.columnconfigure(0, weight=1)

        settings_canvas = tk.Canvas(settings_container, borderwidth=0, highlightthickness=0)
        settings_vscroll = ttk.Scrollbar(settings_container, orient='vertical', command=settings_canvas.yview)
        settings_canvas.configure(yscrollcommand=settings_vscroll.set)
        settings_vscroll.grid(row=0, column=1, sticky='ns')
        settings_canvas.grid(row=0, column=0, sticky='nsew')

        # Inner frame that holds actual settings widgets
        settings_content_frame = ttk.Frame(settings_canvas)
        settings_window = settings_canvas.create_window((0, 0), window=settings_content_frame, anchor='nw')

        def _on_settings_frame_configure(event):
            # Update scrollregion to encompass the inner frame
            settings_canvas.configure(scrollregion=settings_canvas.bbox('all'))

        def _on_settings_canvas_configure(event):
            # Keep the inner frame width fixed to the canvas' width to avoid horizontal stretch of child widgets
            canvas_width = event.width
            settings_canvas.itemconfig(settings_window, width=canvas_width)

        settings_content_frame.bind('<Configure>', _on_settings_frame_configure)
        settings_canvas.bind('<Configure>', _on_settings_canvas_configure)


        # --- Map Marker Icon Section (Single, Optimized) ---
        marker_icon_section_frame = ttk.Frame(settings_content_frame)
        marker_icon_section_frame.pack(fill='x', pady=10, anchor='w', padx=10)
        ttk.Label(marker_icon_section_frame, text="Map Marker Icon:", font=("-weight", "bold")).pack(anchor='w', pady=(0, 5))
        ttk.Label(marker_icon_section_frame, text=f"Select PNG image for map markers. Saved as marker_icon.png in:\n{DATA_DIR}").pack(anchor='w', pady=(0, 10))
        select_marker_icon_button_frame = ttk.Frame(marker_icon_section_frame)
        select_marker_icon_button_frame.pack(fill='x', pady=5, anchor='w')
        select_marker_button = ttk.Button(select_marker_icon_button_frame, text="Select Marker Icon File...", command=self.select_marker_icon)
        select_marker_button.pack(side='left')
        # Canvas for marker icon preview (smaller size)
        self.marker_icon_preview_canvas = tk.Canvas(marker_icon_section_frame, width=50, height=50, bg="lightgrey", relief="sunken")
        self.marker_icon_preview_canvas.pack(pady=10, anchor='w')
        # Initial preview update is now called in __init__ after load_marker_icon_image

        # --- Report Header Logo Section ---
        logo_section_frame = ttk.Frame(settings_content_frame)
        logo_section_frame.pack(fill='x', pady=10, anchor='w', padx=10)
        ttk.Label(logo_section_frame, text="Header Logo:", font=("-weight", "bold")).pack(anchor='w', pady=(0, 5))
        ttk.Label(logo_section_frame, text=f"Select image (png, jpg, jpeg, gif).\nSaved as logo.png in:\n{DATA_DIR}").pack(anchor='w', pady=(0, 10))
        select_logo_button_frame = ttk.Frame(logo_section_frame)
        select_logo_button_frame.pack(fill='x', pady=5, anchor='w')
        select_button = ttk.Button(select_logo_button_frame, text="Select Logo File...", command=self.select_logo)
        select_button.pack(side='left')
        # Canvas for logo preview
        self.logo_preview_canvas = tk.Canvas(logo_section_frame, width=200, height=100, bg="lightgrey", relief="sunken")
        self.logo_preview_canvas.pack(pady=10, anchor='w')
        # Initial preview update is now called in __init__ after load_logo_image


        # --- Action Buttons Frame (packed left) ---
        buttons_area_frame = ttk.Frame(settings_content_frame)
        buttons_area_frame.pack(fill='x', pady=10, anchor='w', padx=10)

        import_button = ttk.Button(buttons_area_frame, text="Import Cases from XLSX", command=self.import_cases_from_xlsx)
        import_button.pack(side='left', pady=(5,0), padx=(0,5))

        log_button = ttk.Button(buttons_area_frame, text="View Application Log", command=self.show_application_log)
        log_button.pack(side='left', pady=(5,0), padx=(0,5))

        header_button = ttk.Button(buttons_area_frame, text="Edit Report Header Info", command=self.show_report_header_info_settings)
        header_button.pack(side='left', pady=(5,0), padx=(0,5))

        change_pw_button = ttk.Button(buttons_area_frame, text="Change Password", command=self.change_password_prompt)
        change_pw_button.pack(side='left', pady=(5,0), padx=(0,5))

        clear_data_button = ttk.Button(buttons_area_frame, text="Clear Application Data", command=self.clear_application_data_prompt, style="Danger.TButton")
        clear_data_button.pack(side='left', pady=(5,0), padx=(0,5))


        # Display default password notice and warning with theme-aware style
        pw_text = (
            f"Default Password: {DEFAULT_PASSWORD}\nIt is highly recommended to change the default password for security."
            if has_password_set() and verify_password(DEFAULT_PASSWORD)
            else "A custom password is set. Use the 'Change Password' button to update it."
        )
        try:
            # Use ttkbootstrap style for better contrast across themes
            password_warning_label = ttk.Label(
                settings_content_frame,
                text=pw_text,
                style="Warning.TLabel"
            )
        except Exception:
            # Fallback for plain ttk
            password_warning_label = ttk.Label(
                settings_content_frame,
                text=pw_text,
                foreground="orange"
            )
        password_warning_label.pack(pady=(15, 0), padx=10, anchor='w')

        # Note about Geocoding limits
        geocoding_note_label = ttk.Label(settings_content_frame,
                                           text="Note: Map geocoding uses Nominatim, which has usage policies.\nPlease use responsibly.",
                                           foreground="gray")
        geocoding_note_label.pack(pady=(5, 0), padx=10, anchor='w')

        # --- Theme Selection Section ---
        theme_section_frame = ttk.Frame(settings_content_frame)
        theme_section_frame.pack(fill='x', pady=10, anchor='w', padx=10)

        ttk.Label(theme_section_frame, text="Application Theme:", font=("Segoe UI", 10, "bold")).pack(anchor='w', pady=(0, 5))


        self.theme_var = tk.StringVar()
        theme_names = [name for name, _ in THEME_OPTIONS]
        theme_values = {name: code for name, code in THEME_OPTIONS}
        theme_codes_to_names = {code: name for name, code in THEME_OPTIONS}

        # Ensure we use ttkbootstrap's native widget for consistent sizing
        # Create a dedicated style to increase internal padding and arrow size
        try:
            # Increase internal padding substantially
            self.style.configure('ThemeSelect.TCombobox', padding=(14, 12, 14, 12))
            # Some themes honor fieldpadding specifically for entry/combobox
            try:
                self.style.configure('ThemeSelect.TCombobox', fieldpadding=(14, 12, 14, 12))
            except Exception:
                pass
            # Some themes honor arrowsize
            try:
                self.style.configure('ThemeSelect.TCombobox', arrowsize=18)
            except Exception:
                pass
            # Make sure there's a visible border in flat themes
            try:
                self.style.configure('ThemeSelect.TCombobox', borderwidth=2, relief='solid')
            except Exception:
                pass
        except Exception as e:
            logging.debug(f"Combobox style configuration skipped: {e}")

        # Set combobox to match saved theme code
        saved_theme_code = getattr(self, '_saved_theme_code', None)
        if saved_theme_code and saved_theme_code in theme_codes_to_names:
            self.theme_var.set(theme_codes_to_names[saved_theme_code])
        else:
            # fallback to current style or first
            current_theme_code = self.root.style.theme.name if hasattr(self.root, 'style') and hasattr(self.root.style, 'theme') else None
            if current_theme_code and current_theme_code in theme_codes_to_names:
                self.theme_var.set(theme_codes_to_names[current_theme_code])
            else:
                self.theme_var.set(theme_names[0])

        theme_combo = tb.Combobox(
            theme_section_frame,
            textvariable=self.theme_var,
            values=theme_names,
            state="readonly",
            width=24,
            style='ThemeSelect.TCombobox',
            bootstyle='secondary'
        )
        # Force normal height by setting an explicit font (affects widget height)
        try:
            theme_combo.configure(font=("Segoe UI", 12))
        except Exception:
            # Fallback to a generic font name if Segoe UI unavailable
            try:
                theme_combo.configure(font=("TkDefaultFont", 12))
            except Exception:
                pass

        # Also enforce font at style level to influence control metrics
        try:
            self.style.configure('ThemeSelect.TCombobox', font=("Segoe UI", 12))
        except Exception:
            pass

        # Keep a compact, fixed-size control; do not expand with window width
        theme_combo.pack(anchor='w', pady=(0, 5))

        def on_theme_change(event=None):
            selected_name = self.theme_var.get()
            selected_code = theme_values[selected_name]
            self.root.style.theme_use(selected_code)
            set_user_pref('theme', selected_code)
            self._saved_theme_code = selected_code

        theme_combo.bind("<<ComboboxSelected>>", on_theme_change)


    # --- Data Handling and UI Refresh ---

    def submit_case(self):
        """Collects data from the entry form and either adds a new case or updates an existing one."""
        case_data = self.collect_form_data(for_validation=True) # Use helper to collect and strip/format

        # --- Validation ---
        case_number = case_data.get("case_number", "").strip()

        # Validate and convert volume_size_gb to float or None
        vol_size_str = case_data.get('volume_size_gb', '').strip()
        if vol_size_str:
             try:
                 case_data['volume_size_gb'] = float(vol_size_str)
             except ValueError:
                 Messagebox.show_info("Validation Error", "Volume Size (GB) must be a valid number.")
                 logging.warning(f"Submit failed: Invalid Volume Size (GB) '{vol_size_str}'.")
                 return # Stop if invalid number
        else:
             case_data['volume_size_gb'] = None # Store as None if empty

        # Handle 'data_recovered' - it comes as boolean from the checkbox now
        # Convert boolean to "Yes", "No", or "" string for database storage
        dr_val = case_data.get('data_recovered') # This is True/False
        # Fix: Only convert to empty string if the value is None, not False
        if dr_val is True:
            case_data['data_recovered'] = "Yes"
        elif dr_val is False:
            case_data['data_recovered'] = "No"
        else:
            case_data['data_recovered'] = ""

        # Ensure fpr_complete is handled correctly (already was BooleanVar)
        # submit_case handles this conversion to 0/1 for DB before insertion/update


        # --- Insert or Update based on self.editing_case_id ---
        if self.editing_case_id is not None:
            # We are editing an existing case
            case_id_to_update = self.editing_case_id
            logging.info(f"Attempting to update case ID: {case_id_to_update}")

            # Pass the collected case_data dictionary directly to update_case_db
            # update_case_db handles converting boolean fpr_complete to 0/1 for update
            # --- Undo/Redo support: Save old data before update ---
            old_case = get_case_by_id_db(case_id_to_update)
            if update_case_db(case_id_to_update, case_data):
                self.push_view_edit_history(old_case, case_data)
                Messagebox.show_info("Success", f"Case ID {case_id_to_update} updated successfully.")
                logging.info(f"Case ID {case_id_to_update} updated.")
                self.clear_entry_form() # Clear form and reset editing state
                self.refresh_data_view() # Refresh the view to show changes
                # Reload map markers and graphs as data might affect them
                if hasattr(self, 'map_widget'):
                     self.load_map_markers() # This will start a new threaded load
                self.populate_graph_filters() # This also calls update_graph
                self.update_status(f"Case ID {case_id_to_update} updated.")

            else:
                # Error message shown by update_case_db logging
                Messagebox.show_error("Database Error", f"Failed to update case ID {case_id_to_update}. See log for details.")
                self.update_status(f"Failed to update case ID {case_id_to_update}.")


        else:
            # We are adding a new case
            logging.info(f"Attempting to submit new case: {case_number}")
            # Pass the collected case_data dictionary directly to add_case_db
            # add_case_db handles the bool to int conversion for insert
            if add_case_db(case_data): # add_case_db returns True/False
                Messagebox.show_info("Success", "Case submitted successfully.")
                logging.info(f"New case '{case_number}' submitted.")
                self.clear_entry_form() # Clear form after successful submission
                self.refresh_data_view() # Refresh the view to show the new case
                # Reload map markers and graphs for the new data
                if hasattr(self, 'map_widget'):
                     self.load_map_markers() # This will start a new threaded load
                self.populate_graph_filters() # This also calls update_graph
                self.update_status(f"New case '{case_number}' submitted.")

            else:
                # Error message shown by add_case_db logging (e.g., duplicate if somehow missed get_case_by_number_db)
                Messagebox.show_error("Database Error", f"Failed to submit case '{case_number}'. It may already exist. See log for details.")
                self.update_status(f"Failed to submit case '{case_number}'.")

        # Before/after adding the case, update combo values for persistent fields
        for key in ["examiner", "investigator", "agency", "offense_type", "city_of_offense"]:
            if key in self.entries and isinstance(self.entries[key], tk.StringVar):
                value = self.entries[key].get().strip()
                if value:
                    values = get_combo_values_db(key)
                    if value not in values:
                        values.append(value)
                        set_combo_values_db(key, values)

        # No matter if insert or update, refresh related parts of the UI
        # Already done within the if/else blocks above


    def collect_form_data(self, for_validation=True):
        """Collects data from the entry form widgets into a dictionary.
           Handles different widget types.
           Use for_validation=False to collect raw values without stripping."""
        case_data = {}
        for key, widget in self.entries.items():
            if isinstance(widget, ttk.Entry):
                value = widget.get().strip() if for_validation else widget.get()
                case_data[key] = value
            elif isinstance(widget, tk.StringVar): # Combobox StringVar
                value = widget.get().strip() if for_validation else widget.get()
                case_data[key] = value
            elif isinstance(widget, tk.BooleanVar): # Checkbutton BooleanVar
                case_data[key] = widget.get() # This returns True/False directly
            elif isinstance(widget, tk.Text): # Text widget for Notes
                # Get text from 1.0 to end-1c (to exclude the trailing newline)
                value = widget.get("1.0", "end-1c").strip() if for_validation else widget.get("1.0", "end-1c")
                case_data[key] = value
            elif isinstance(widget, DateEntry): # DateEntry widget
                date_str = widget.entry.get()
                if date_str:
                    try:
                        # Parse using the widget's dateformat (should be %m-%d-%Y)
                        date_obj = datetime.strptime(date_str, '%m-%d-%Y').date()
                        case_data[key] = date_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        case_data[key] = None
                else:
                    case_data[key] = None
            elif isinstance(widget, tk.StringVar) and key in ["start_date", "end_date"]:
                value = widget.get().strip()
                if value:
                    try:
                        date_obj = datetime.strptime(value, '%m-%d-%Y').date()
                        case_data[key] = date_obj.strftime('%Y-%m-%d')
                    except Exception:
                        case_data[key] = None
                else:
                    case_data[key] = None
            # Add handling for other widget types if any exist
            # else:
            #     logging.warning(f"Unknown widget type for key '{key}' during data collection: {type(widget)}")

        return case_data


    def clear_entry_form(self):
        """Clears all input fields and resets editing state."""
        self.editing_case_id = None
        if self.submit_button:
            self.submit_button.config(text="Submit Case", style="Accent.TButton")
        if hasattr(self, 'notebook') and hasattr(self, 'entry_frame'):
            self.notebook.tab(self.entry_frame, text="New Case Entry")


        for key, widget in self.entries.items():
            if isinstance(widget, ttk.Entry):
                widget.delete(0, tk.END)
            elif isinstance(widget, tk.StringVar):
                combo_widget = None
                if hasattr(self, 'field_frame_container'):
                    for child in self.field_frame_container.winfo_children():
                        for grandchild in child.winfo_children():
                            if isinstance(grandchild, ttk.Combobox) and grandchild.cget('textvariable') == str(widget):
                                combo_widget = grandchild
                                break
                        if combo_widget:
                            break
                if combo_widget:
                    current_values = combo_widget.cget('values')
                    if key == "state_of_offense" and "MS" in current_values:
                        widget.set("MS")
                    elif current_values:
                        widget.set(current_values[0])
                    else:
                        widget.set('')
                else:
                    widget.set('')
            elif isinstance(widget, tk.BooleanVar):
                widget.set(False)
            elif isinstance(widget, tk.Text):
                widget.delete('1.0', tk.END)
            elif isinstance(widget, DateEntry):
                try:
                    widget.entry.delete(0, tk.END)
                    widget._set_text("")
                except:
                    pass

        # Auto-populate Examiner with last used value
        last_examiner = self.get_last_examiner()
        if last_examiner and 'examiner' in self.entries and isinstance(self.entries['examiner'], tk.StringVar):
            self.entries['examiner'].set(last_examiner)

    # Removed duplicate/broken load_map_markers. The correct version is defined earlier in the class.

    def load_logo_image(self):
        """Loads and scales the logo image for use in the application."""
        try:
            # Load and scale logo for entry tab
            image = Image.open(self.logo_path.get())
            # Scale to reasonable height (100px) maintaining aspect ratio
            aspect_ratio = image.size[0] / image.size[1]
            new_height = 100
            new_width = int(new_height * aspect_ratio)
            image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
            self.logo_image_tk = ImageTk.PhotoImage(image)

            # Update overlay logo label in entry tab
            if self.entry_logo_label:
                self.entry_logo_label.config(image=self.logo_image_tk)
                try:
                    self.entry_logo_label.lift()
                except Exception:
                    pass

            # Create smaller version for settings preview
            preview_height = 100
            preview_width = int(preview_height * aspect_ratio)
            preview_image = image.resize((preview_width, preview_height), Image.Resampling.LANCZOS)
            self.logo_image_tk_preview = ImageTk.PhotoImage(preview_image)

            # Update preview in settings if canvas exists
            if self.logo_preview_canvas:
                self.logo_preview_canvas.delete("all")
                # Center the image in the canvas
                x = (200 - preview_width) // 2  # 200 is canvas width
                self.logo_preview_canvas.create_image(x, 0, anchor='nw', image=self.logo_image_tk_preview)

            logging.info(f"Logo loaded successfully from {self.logo_path.get()}")
        except Exception as e:
            logging.warning(f"Could not load logo image: {e}")
            # Clear existing images if load fails
            self.logo_image_tk = None
            self.logo_image_tk_preview = None
            if self.entry_logo_label:
                self.entry_logo_label.config(image='', text='')
            if self.logo_preview_canvas:
                self.logo_preview_canvas.delete("all")
                self.logo_preview_canvas.create_text(100, 50, text="No Logo", anchor='center')

    def load_marker_icon_image(self):
        """Loads and scales the marker icon image for use in the application."""
        try:
            # Load and scale marker icon for map markers
            image = Image.open(MARKER_ICON_FILENAME)
            # Scale to 20x20 for map markers
            map_image = image.resize((20, 20), Image.Resampling.LANCZOS)
            self.marker_icon_tk_map = ImageTk.PhotoImage(map_image)

            # Create larger version for settings preview (50x50)
            preview_image = image.resize((50, 50), Image.Resampling.LANCZOS)
            self.marker_icon_tk_preview = ImageTk.PhotoImage(preview_image)

            # Update preview in settings if canvas exists
            if self.marker_icon_preview_canvas:
                self.marker_icon_preview_canvas.delete("all")
                # Center the image in the canvas
                x = (50 - 50) // 2   # Canvas width - image width
                self.marker_icon_preview_canvas.create_image(x, 0, anchor='nw', 
                                                           image=self.marker_icon_tk_preview)

            # Set the global marker icon for map markers
            global DEFAULT_MARKER_ICON
            DEFAULT_MARKER_ICON = self.marker_icon_tk_map

            logging.info(f"Marker icon loaded successfully from {MARKER_ICON_FILENAME}")
        except Exception as e:
            logging.warning(f"Could not load marker icon image: {e}")
            # Clear existing images if load fails
            self.marker_icon_tk_map = None
            self.marker_icon_tk_preview = None
            if self.marker_icon_preview_canvas:
                self.marker_icon_preview_canvas.delete("all")
                self.marker_icon_preview_canvas.create_text(25, 25, 
                                                          text="No Icon", 
                                                          anchor='center')

    def export_pdf_report(self):
        """Export all cases to a PDF report with improved formatting and word wrapping."""
        # Ask user for save location
        filename = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf")],
            title="Save PDF Report As"
        )
        if not filename:
            return

        try:
            self.update_status("Generating PDF report...")
            
            # Get all cases
            cases = get_all_cases_db()
            
            # For PDF reports, include ALL columns except 'id' (regardless of treeview visibility)
            # This ensures that important fields like 'data_recovered' are always included
            headers = [
                config["text"] for key, config in self.tree_columns_config.items()
                if key != 'id'  # Only exclude 'id', include everything else
            ]
            
            # Get the keys for column mapping
            visible_keys = [
                key for key, config in self.tree_columns_config.items()
                if key != 'id'  # Only exclude 'id', include everything else
            ]
            
            
            # Process data with proper formatting and word wrapping
            from reportlab.platypus import Paragraph
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.enums import TA_LEFT
            
            styles = getSampleStyleSheet()
            # Create a custom style for table cells with smaller font and better wrapping
            cell_style = ParagraphStyle(
                'CellStyle',
                parent=styles['Normal'],
                fontSize=7,
                leading=8,
                leftIndent=0,
                rightIndent=0,
                spaceAfter=0,
                spaceBefore=0,
                alignment=TA_LEFT,
                wordWrap='LTR'
            )
            
            data = []
            for case in cases:
                row = []
                for key in visible_keys:
                    value = case.get(key, '')
                    
                    # Format specific field types
                    if key in ['start_date', 'end_date', 'created_at']:
                        value = format_date_str_for_display(value)
                    elif key == 'fpr_complete':
                        value = format_bool_int(value)
                    elif key == 'data_recovered':
                        # Ensure data_recovered shows correctly - handle all possible values
                        if value in ['Yes', 'No']:
                            value = value
                        elif value == '':
                            value = ""  # Show empty for empty strings
                        else:
                            value = str(value)  # Convert any other value to string
                    
                    # Convert all values to strings first
                    value = str(value) if value is not None else ""
                    
                    # For better formatting, wrap ALL text content in Paragraph objects
                    # This ensures consistent formatting and proper word wrapping
                    if value:
                        # Use Paragraph for all non-empty content to enable proper word wrapping
                        value = Paragraph(value, cell_style)
                    else:
                        # Even empty values should be Paragraphs for consistent table formatting
                        value = Paragraph("", cell_style)
                    
                    row.append(value)
                data.append(row)
            
            # Determine optimal page orientation based on number of columns
            num_cols = len(headers)
            
            # Use landscape for more than 8 columns, portrait otherwise
            use_landscape = num_cols > 8
            pagesize = landscape(letter) if use_landscape else letter
            
            # Create the PDF document with narrower margins
            doc = SimpleDocTemplate(
                filename,
                pagesize=pagesize,
                leftMargin=0.4*inch,
                rightMargin=0.4*inch,
                topMargin=0.5*inch,
                bottomMargin=0.5*inch,
            )

            # Prepare content elements
            elements = []

            # Header lines (left) and logo top-right with title on one line
            header_info = self.get_report_header_info()
            header_lines = [
                f"Name: {header_info.get('Name','')}",
                f"Agency: {header_info.get('Agency','')}",
                f"Division: {header_info.get('Division','')}",
                f"Date: {datetime.now().strftime('%Y-%m-%d')}"
            ]
            elements.extend(self._pdf_build_header(
                title_text="Case Log Report",
                doc_width=doc.width,
                has_logo=True,
                right_logo_width=1.1*inch,
                right_logo_height=1.1*inch,
                left_info_lines=header_lines,
            ))

            # Calculate dynamic column widths based on content and page size
            page_width = doc.width
            
            # Define column width preferences based on field type and content
            col_width_preferences = {
                'case_number': 0.8,
                'examiner': 1.0,
                'investigator': 1.0,
                'agency': 1.2,
                'city_of_offense': 1.0,
                'state_of_offense': 0.5,
                'start_date': 0.8,
                'end_date': 0.8,
                'volume_size_gb': 0.6,
                'offense_type': 1.5,
                'device_type': 0.8,
                'model': 1.0,
                'os': 0.6,
                'data_recovered': 0.6,  # Ensure this gets proper width
                'fpr_complete': 0.5,
                'created_at': 0.8,
                'notes': 2.5  # Notes get more space for word wrapping
            }
            
            # Calculate column widths proportionally
            total_weight = sum(col_width_preferences.get(key, 1.0) for key in visible_keys)
            col_widths = []
            for key in visible_keys:
                weight = col_width_preferences.get(key, 1.0)
                width = (weight / total_weight) * page_width
                # Set minimum and maximum widths to prevent columns from being too narrow or too wide
                min_width = 0.4 * inch
                max_width = 2.5 * inch
                col_widths.append(max(min_width, min(width, max_width)))
            
            # Create table with headers and data
            table_data = [headers] + data
            
            # Create the table with improved settings for better formatting
            table = Table(
                table_data, 
                colWidths=col_widths, 
                repeatRows=1,  # Repeat header on each page
                splitByRow=True  # Allow table to split across pages
            )
            
            
            # Apply comprehensive styling with improved word wrapping and formatting
            table.setStyle(TableStyle([
                # Header styling
                ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
                
                # Data row styling - using smaller font for better fit
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 7),  # Reduced from 8 to 7
                ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 1), (-1, -1), 'TOP'),  # Align to top for better readability
                
                # Grid and borders
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.darkblue),  # Thicker line under header
                
                # Padding for better spacing and readability
                ('LEFTPADDING', (0, 0), (-1, -1), 3),
                ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),  # More bottom padding for wrapped text
                
                # Word wrapping and text flow settings
                ('WORDWRAP', (0, 0), (-1, -1), True),
                ('SPLITLONGWORDS', (0, 0), (-1, -1), True),
            ]))
            
            # Add alternating row colors for better readability
            for i in range(1, len(table_data)):
                if i % 2 == 0:
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, i), (-1, i), colors.Color(0.95, 0.95, 0.95)),  # Light gray
                    ]))

            elements.append(table)
            
            # Build the PDF with footer
            gen_info = self.get_report_header_info()
            gen_name = gen_info.get('Name','')
            gen_date = datetime.now().strftime('%Y-%m-%d %H:%M')
            doc.build(
                elements,
                onFirstPage=lambda c,d: self._pdf_footer(c, d, gen_name, gen_date),
                onLaterPages=lambda c,d: self._pdf_footer(c, d, gen_name, gen_date)
            )
            
            self.update_status("PDF report generated successfully.")
            Messagebox.show_info("Success", f"PDF report saved to:\n{filename}")
            
        except Exception as e:
            logging.error(f"Error generating PDF report: {e}")
            self.update_status("Error generating PDF report.")
            Messagebox.show_error("Error", f"Failed to generate PDF report: {e}")

    def export_xlsx_report(self):
        """Export all cases to an XLSX spreadsheet."""
        # Ask user for save location
        filename = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            title="Save Excel Report As"
        )
        if not filename:
            return

        try:
            self.update_status("Generating Excel report...")
            
            # Get all cases
            cases = get_all_cases_db()
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(cases)
            
            # Reorder columns based on tree_columns_config
            visible_columns = [
                key for key, config in self.tree_columns_config.items()
                if config.get("visible", True)
            ]
            df = df[visible_columns]
            
            # Rename columns using display text from tree_columns_config
            column_names = {
                key: config["text"]
                for key, config in self.tree_columns_config.items()
                if config.get("visible", True)
            }
            df = df.rename(columns=column_names)
            
            # Export to Excel
            df.to_excel(filename, index=False)
            
            self.update_status("Excel report generated successfully.")
            Messagebox.show_info("Success", f"Excel report saved to:\n{filename}")
            
        except Exception as e:
            logging.error(f"Error generating Excel report: {e}")
            self.update_status("Error generating Excel report.")
            Messagebox.show_error("Error", f"Failed to generate Excel report: {e}")

    def edit_selected_case(self):
        """Loads the selected case into the entry form for editing."""
        # Get the selected item from the treeview
        selected_items = self.tree.selection()
        if not selected_items:
            Messagebox.show_info("Select Case", "Please select a case to edit.")
            return

        # We'll edit only the first selected item if multiple are selected
        item_id = selected_items[0]
        # The treeview item ID may be a string (e.g., 'I001'), but the DB expects an integer primary key.
        # Get the actual DB id from the first column of the treeview values:
        try:
            values = self.tree.item(item_id, 'values')
            if not values:
                Messagebox.show_error("Error", "Could not retrieve case data from selection.")
                return
            db_id = values[0]
            logging.info(f"Attempting to retrieve case for editing with DB ID: {db_id}")

            # Get the case data from the database
            case_data = get_case_by_id_db(db_id)
            if not case_data:
                Messagebox.show_error("Error", f"Could not find case with ID {db_id}")
                return

            # Switch to the entry tab
            self.notebook.select(self.entry_frame)
            # Clear the form and populate with case data
            self.clear_entry_form() # This also resets editing_case_id, set it again below
            self.populate_entry_form(case_data) # Populate the form with retrieved data

            # Set editing state
            self.editing_case_id = db_id
            self.submit_button.config(text="Update Case")
            self.notebook.tab(self.entry_frame, text="Edit Case")

        except Exception as e:
            logging.error(f"Error in edit_selected_case for item {item_id}:\n{e}")
            Messagebox.show_error("Error", f"Failed to load case for editing: {e}")

    def populate_entry_form(self, case_data):
        """Populates the entry form with the provided case data."""
        for key, widget in self.entries.items():
            value = case_data.get(key)
            
            if isinstance(widget, ttk.Entry):
                widget.delete(0, tk.END)
                if value is not None:  # Only set if value exists
                    widget.insert(0, str(value))
                    
            elif isinstance(widget, tk.StringVar):
                if value is not None:
                    widget.set(str(value))
                else:
                    widget.set('')
                
            elif isinstance(widget, tk.BooleanVar):
                if key == 'fpr_complete':
                    widget.set(bool(value))  # Convert 0/1 to False/True
                elif key == 'data_recovered':
                    widget.set(value == "Yes")  # Convert "Yes"/"No"/"" to True/False
                
            elif isinstance(widget, tk.Text):
                widget.delete('1.0', tk.END)
                if value:
                    widget.insert('1.0', str(value))
                
            elif isinstance(widget, DateEntry):
                try:
                    if value:
                        date_obj = datetime.strptime(value, '%Y-%m-%d').date()
                        widget.entry.delete(0, tk.END)
                        widget.entry.insert(0, date_obj.strftime('%m-%d-%Y'))
                    else:
                        widget.entry.delete(0, tk.END)
                except Exception:
                    widget.entry.delete(0, tk.END)
    
    def delete_selected_cases(self):
        """Deletes selected cases from the database after confirmation."""
        # Get the selected items from the treeview
        selected_items = self.tree.selection()
        if not selected_items:
            Messagebox.show_info("Select Cases", "Please select one or more cases to delete.")
            return

        # Ask for password confirmation
        pw = simpledialog.askstring("Password Required", "Enter password to confirm deletion:", show="*")
        if not pw:
            self.update_status("Delete cancelled (no password entered).")
            return
        if not verify_password(pw):
            Messagebox.show_error("Authentication Failed", "Incorrect password. Cases were not deleted.")
            self.update_status("Delete cancelled (incorrect password).")
            return

        # Extract DB IDs from the first value column of each selected row
        db_ids = []
        for item in selected_items:
            try:
                values = self.tree.item(item, 'values') or ()
                if not values:
                    continue
                # First column holds the DB primary key
                db_id = int(values[0])
                db_ids.append(db_id)
            except Exception:
                logging.warning(f"Could not determine DB id for tree item {item} with values={values}")
        if not db_ids:
            Messagebox.show_error("Delete Error", "Couldn't determine the selected case IDs to delete.")
            return

        # Ask for final confirmation with count of selected DB IDs
        confirm = messagebox.askyesno(
            "Confirm Deletion",
            f"Are you sure you want to delete {len(db_ids)} selected case(s)?\nThis cannot be undone."
        )
        if not confirm:
            self.update_status("Delete cancelled by user.")
            return

        # Try to delete each selected case
        deleted_count = 0
        failed_count = 0

        try:
            for case_id in db_ids:
                if delete_case_db(case_id):
                    deleted_count += 1
                else:
                    failed_count += 1
                    logging.error(f"Failed to delete case ID {case_id}")

            # Refresh the view after deletions
            self.refresh_data_view()
            if hasattr(self, 'map_widget'):
                self.load_map_markers()
            self.populate_graph_filters()
            
            # Show results
            status = f"Deleted {deleted_count} case(s)"
            if failed_count:
                status += f", {failed_count} failed"
            self.update_status(status)
            
            Messagebox.show_info(
                "Delete Complete",
                f"Successfully deleted {deleted_count} case(s).\n"
                f"Failed to delete {failed_count} case(s)."
            )

        except Exception as e:
            logging.error(f"Error during case deletion: {e}")
            Messagebox.show_error(
                "Error",
                f"An error occurred while deleting cases: {e}"
            )
            self.update_status("Error during case deletion.")

    def select_logo(self):
        """Opens a file dialog to select a new logo image file."""
        filetypes = [
            ('Image files', '*.png *.jpg *.jpeg *.gif'),
            ('PNG files', '*.png'),
            ('JPEG files', '*.jpg *.jpeg'),
            ('GIF files', '*.gif'),
            ('All files', '*.*')
        ]
        
        filename = filedialog.askopenfilename(
            title="Select Logo Image",
            filetypes=filetypes,
            initialdir=os.path.dirname(self.logo_path.get())
        )
        
        if not filename:
            return  # User cancelled
            
        try:
            # Copy selected file to app_data directory as logo.png
            img = Image.open(filename)
            img.save(LOGO_FILENAME, 'PNG')  # Always save as PNG
            
            # Update logo path and reload
            self.logo_path.set(LOGO_FILENAME)
            self.load_logo_image()
            
            logging.info(f"New logo selected and saved: {filename}")
            self.update_status("Logo updated successfully.")
            
        except Exception as e:
            logging.error(f"Error setting new logo: {e}")
            Messagebox.show_error(
                "Logo Error",
                f"Could not set new logo:\n{str(e)}"
            )
            self.update_status("Error updating logo.")

    def select_marker_icon(self):
        """Opens a file dialog to select a new marker icon image file."""
        filetypes = [
            ('Image files', '*.png *.jpg *.jpeg *.gif'),
            ('PNG files', '*.png'),
            ('JPEG files', '*.jpg *.jpeg'),
            ('GIF files', '*.gif'),
            ('All files', '*.*')
        ]
        
        filename = filedialog.askopenfilename(
            title="Select Marker Icon Image",
            filetypes=filetypes,
            initialdir=os.path.dirname(MARKER_ICON_FILENAME)
        )
        
        if not filename:
            return  # User cancelled
            
        try:
            # Copy selected file to app_data directory as marker_icon.png
            img = Image.open(filename)
            img.save(MARKER_ICON_FILENAME, 'PNG')  # Always save as PNG
            
            # Reload marker icon
            self.load_marker_icon_image()
            
            # If map is loaded, refresh markers with new icon
            if hasattr(self, 'map_widget'):
                self.load_map_markers()
            
            logging.info(f"New marker icon selected and saved: {filename}")
            self.update_status("Marker icon updated successfully.")
            
        except Exception as e:
            logging.error(f"Error setting new marker icon: {e}")
            Messagebox.show_error(
                "Marker Icon Error",
                f"Could not set new marker icon:\n{str(e)}"
            )
            self.update_status("Error updating marker icon.")

    def import_cases_from_xlsx(self):
        """Imports case data from a selected XLSX file."""
        file_path = filedialog.askopenfilename(
            title="Select XLSX File",
            filetypes=[("Excel files", "*.xlsx")],
            initialdir=os.path.dirname(DATA_DIR)
        )
        if not file_path:
            self.update_status("XLSX import cancelled.")
            logging.info("XLSX import cancelled by user.")
            return

        try:
            self.update_status("Importing cases from XLSX...")
            self.progress.grid()  # Show progress bar using grid
            self.progress.start(10)  # Start progress animation
            
            # Read the Excel file
            df = pd.read_excel(file_path, engine='openpyxl')
            excel_columns = [str(col).strip() for col in df.columns]
            
            # Build mapping from Excel headers to DB keys
            excel_header_to_db_key = {}
            for col_key, config in self.tree_columns_config.items():
                if col_key in ['id', 'created_at']:  # Skip these columns
                    continue
                display_text = config.get("text", col_key)
                for excel_col in excel_columns:
                    if excel_col.strip().lower() in [display_text.strip().lower(), col_key.strip().lower()]:
                        excel_header_to_db_key[excel_col] = col_key
                        break

            imported_count = 0
            skipped_count = 0

            # Process each row
            for idx, row in df.iterrows():
                case_data = {}
                for excel_col, db_key in excel_header_to_db_key.items():
                    value = row.get(excel_col)
                    
                    # Handle dates
                    if db_key in ['start_date', 'end_date'] and pd.notnull(value):
                        try:
                            value = pd.to_datetime(value).strftime('%Y-%m-%d')
                        except:
                            value = None
                
                    # Convert boolean fields
                    elif db_key == 'fpr_complete':
                        if isinstance(value, bool):
                            value = 1 if value else 0
                        elif pd.notnull(value):
                            value = 1 if str(value).lower() in ['yes', 'true', '1'] else 0
                        else:
                            value = 0
                
                    # Handle data_recovered field
                    elif db_key == 'data_recovered':
                        if isinstance(value, bool):
                            value = "Yes" if value else "No"
                        elif pd.notnull(value):
                            value = "Yes" if str(value).lower() in ['yes', 'true', '1'] else "No"
                        else:
                            value = ""
                
                    # Handle all other fields
                    case_data[db_key] = value if pd.notnull(value) else None

                # Try to add the case
                if add_case_db(case_data):
                    imported_count += 1
                    self.update_status(f"Imported {imported_count} cases...")
                else:
                    skipped_count += 1
                    logging.warning(f"Row {idx+1} failed to add: {case_data}")

            # Refresh UI after import
            self.refresh_data_view()
            # self.create_dashboard_widgets()  # Removed: dashboard no longer exists
            if hasattr(self, 'map_widget'):
                self.load_map_markers()
            self.populate_graph_filters()

            # Show results
            Messagebox.show_info(
                "Import Complete",
                f"Import finished.\nImported: {imported_count}\nSkipped: {skipped_count}"
            )

        except Exception as e:
            logging.error(f"Error importing cases from XLSX: {e}")
            Messagebox.show_error(
                "Import Error",
                f"Failed to import cases: {e}"
            )
        finally:
            self.progress.stop()  # Stop progress animation
            self.progress.grid_remove()  # Hide progress bar
            self.update_status(f"Import complete. {imported_count} imported, {skipped_count} skipped.")

    def show_application_log(self):
        """Shows the application log file in a scrollable window."""
        try:
            # Create a new top-level window
            log_window = tk.Toplevel(self.root)
            log_window.title("Application Log")
            log_window.geometry("800x600")

            # Create a scrolled text widget
            log_text = scrolledtext.ScrolledText(
                log_window, 
                wrap=tk.WORD, 
                width=80, 
                height=30,
                font=("Courier", 10)
            )
            log_text.pack(fill='both', expand=True, padx=10, pady=10)

            # Read and display the log file
            try:
                with open(LOG_FILENAME, 'r', encoding='utf-8') as f:
                    log_content = f.read()
                    log_text.insert('1.0', log_content)
                    log_text.config(state='disabled')  # Make read-only
            except Exception as e:
                log_text.insert('1.0', f"Error reading log file: {e}")
                log_text.config(state='disabled')

            # Add refresh and close buttons
            button_frame = ttk.Frame(log_window)
            button_frame.pack(fill='x', padx=10, pady=(0, 10))

            def refresh_log():
                try:
                    log_text.config(state='normal')
                    log_text.delete('1.0', tk.END)
                    with open(LOG_FILENAME, 'r', encoding='utf-8') as f:
                        log_content = f.read()
                        log_text.insert('1.0', log_content)
                    log_text.config(state='disabled')
                except Exception as e:
                    log_text.config(state='normal')
                    log_text.delete('1.0', tk.END)
                    log_text.insert('1.0', f"Error refreshing log: {e}")
                    log_text.config(state='disabled')

            refresh_btn = ttk.Button(button_frame, text="Refresh", command=refresh_log)
            refresh_btn.pack(side='left', padx=(0, 5))

            close_btn = ttk.Button(button_frame, text="Close", command=log_window.destroy)
            close_btn.pack(side='left')

            # Make the log window transient to the main window (always stays on top of it)
            log_window.transient(self.root)
            
            # Focus the log window
            log_window.focus_set()

        except Exception as e:
            logging.error(f"Error showing application log: {e}")
            Messagebox.show_error(
                "Error",
                f"Could not display application log:\n{str(e)}"
            )

    def change_password_prompt(self):
        """Prompts for old and new passwords and handles the password change."""
        # Verify current password first
        old_pw = simpledialog.askstring(
            "Current Password",
            "Enter current password:",
            show="*"
        )
        if not old_pw:
            return  # User cancelled
        
        if not verify_password(old_pw):
            Messagebox.show_error(
                "Authentication Failed",
                "Incorrect password."
            )
            return
        
        # Get new password
        new_pw = simpledialog.askstring(
            "New Password",
            "Enter new password:",
            show="*"
        )
        if not new_pw:
            return  # User cancelled
        
        # Confirm new password
        confirm_pw = simpledialog.askstring(
            "Confirm Password",
            "Confirm new password:",
            show="*"
        )
        if not confirm_pw:
            return  # User cancelled
        
        # Verify passwords match
        if new_pw != confirm_pw:
            Messagebox.show_error(
                "Password Mismatch",
                "New passwords do not match."
            )
            return
        
        # Update password in database
        if update_password_db(new_pw):
            Messagebox.show_info(
                "Success",
                "Password changed successfully."
            )
            logging.info("Password updated successfully.")
            self.update_status("Password updated successfully.")
        else:
            Messagebox.show_error(
                "Error",
                "Failed to update password. See log for details."
            )
            self.update_status("Failed to update password.")
            
    def clear_application_data_prompt(self):
        """Prompts for password and confirmation before clearing application data."""
        # First verify password
        pw = simpledialog.askstring(
            "Password Required",
            "Enter password to confirm data clearing:",
            show="*"
        )
        if not pw:
            self.update_status("Clear data cancelled (no password entered).")
            return
        
        if not verify_password(pw):
            Messagebox.show_error(
                "Authentication Failed",
                "Incorrect password. Data was not cleared."
            )
            self.update_status("Clear data cancelled (incorrect password).")
            return

        # Ask for explicit confirmation
        confirm = messagebox.askyesno(
            "Confirm Data Clear",
            "WARNING: This will delete ALL cases and reset the application.\n\n"
            "This action cannot be undone!\n\n"
            "Are you absolutely sure you want to continue?",
            icon='warning'
        )
        
        if not confirm:
            self.update_status("Clear data cancelled by user.")
            return

        try:
            # Clear the database
            conn = sqlite3.connect(DB_FILENAME)
            cursor = conn.cursor()
            
            # Drop and recreate case_log table
            cursor.execute("DROP TABLE IF EXISTS case_log")
            cursor.execute('''
                CREATE TABLE case_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_number TEXT,
                    examiner TEXT,
                    offense_type TEXT,
                    device_type TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    volume_size_gb REAL,
                    city_of_offense TEXT,
                    state_of_offense TEXT,
                    investigator TEXT,
                    agency TEXT,
                    model TEXT,
                    os TEXT,
                    data_recovered TEXT,
                    fpr_complete INTEGER,
                    notes TEXT,
                    created_at TEXT
                )
            ''')
            
            # Clear geocache table
            cursor.execute("DELETE FROM geocache")
            
            # Clear combo values from settings (except password)
            cursor.execute("DELETE FROM settings WHERE key LIKE 'combo_%'")
            
            conn.commit()
            conn.close()

            # Clear any saved images
            if os.path.exists(LOGO_FILENAME):
                os.remove(LOGO_FILENAME)
            if os.path.exists(MARKER_ICON_FILENAME):
                os.remove(MARKER_ICON_FILENAME)

            # Reload UI elements
            self.refresh_data_view()
            # self.create_dashboard_widgets()  # Removed: dashboard no longer exists
            if hasattr(self, 'map_widget'):
                self.load_map_markers()
            self.populate_graph_filters()
            self.load_logo_image()
            self.load_marker_icon_image()

            Messagebox.show_info(
                "Success",
                "Application data has been cleared successfully.\n"
                "The application will now close.\n\n"
                "Please restart the application."
            )
            logging.info("Application data cleared successfully.")
            
            # Close the application
            self.root.quit()

        except Exception as e:
            logging.error(f"Error clearing application data: {e}")
            Messagebox.show_error(
                "Error",
                f"Failed to clear application data: {e}"
            )
            self.update_status("Error clearing application data.")
            
    def update_status(self, text, duration=None):
        """
        Updates the status bar text with optional auto-clear after duration.
        Args:
            text (str): Text to display in status bar
            duration (int, optional): Time in ms after which to clear the status
        """
        # Cancel any pending status clear
        if hasattr(self, 'status_animation_id') and self.status_animation_id:
            self.root.after_cancel(self.status_animation_id)
            self.status_animation_id = None

        # Store the text (needed for animation)
        self.status_text = text

        # Update the status label if it exists
        if hasattr(self, 'status_label') and self.status_label:
            self.status_label.config(text=text)

        # Schedule auto-clear if duration specified
        if duration:
            self.status_animation_id = self.root.after(duration, lambda: self.update_status(""))
        
    def populate_graph_filters(self):
        """Populates the graph filters (year dropdown) with available years from the data."""
        try:
            # Get all cases
            cases = get_all_cases_db()
            
            # Extract unique years from start_date
            years = set()
            for case in cases:
                start_date = case.get('start_date', '')
                if start_date and len(start_date) >= 4:  # Ensure we have at least YYYY
                    year = start_date[:4]
                    if year.isdigit():  # Ensure it's a valid year
                        years.add(year)
            
            # Sort years in descending order
            sorted_years = sorted(years, reverse=True)
            
            # Update combobox values
            if hasattr(self, 'graph_year_combo'):
                values = ["All"] + sorted_years
                self.graph_year_combo['values'] = values
                # Keep current selection if valid, otherwise set to "All"
                current = self.graph_year_var.get()
                if current not in values:
                    self.graph_year_var.set("All")
            
            # Update the graph with new filters
            self.update_graph()
            
        except Exception as e:
            logging.error(f"Error populating graph filters: {e}")

    def apply_view_filter(self):
        """Apply the search/filter to the data view."""
        filter_str = self.view_search_var.get().strip().lower()
        self._view_filter_string = filter_str
        self.refresh_data_view()

    def clear_view_filter(self):
        """Clear the search/filter and show all data."""
        self.view_search_var.set("")
        self._view_filter_string = ""
        self.refresh_data_view()

    # Add this method to the CaseLogApp class
    def on_closing(self):
        """Handle cleanup when closing the application."""
        try:
            # Clear references to PhotoImage objects to prevent deletion errors
            if hasattr(self, 'logo_image_tk'):
                self.logo_image_tk = None
            if hasattr(self, 'logo_image_tk_preview'):
                self.logo_image_tk_preview = None
            if hasattr(self, 'marker_icon_tk_map'):
                self.marker_icon_tk_map = None
            if hasattr(self, 'marker_icon_tk_preview'):
                self.marker_icon_tk_preview = None
            
            # Clear the global marker icon
            global DEFAULT_MARKER_ICON
            DEFAULT_MARKER_ICON = None
            
            # Log application shutdown
            logging.info("Application shutting down.")
            
            # Attempt a database backup before closing
            try:
                perform_db_backup(retention_days=56)
            except Exception as e:
                logging.warning(f"Database backup on close failed: {e}")
            
            # Destroy the root window
            self.root.destroy()
            
        except Exception as e:
            logging.error(f"Error during application shutdown: {e}")
            # Try to force close if cleanup fails
            self.root.destroy()
    
if __name__ == "__main__":
    # Initialize the database
    init_db()
    
    # Create the main window with the specified theme
    root = tb.Window(themename="flatly")
    
    # Create the application instance
    app = CaseLogApp(root)
    
    # Start the main event loop
    root.mainloop()