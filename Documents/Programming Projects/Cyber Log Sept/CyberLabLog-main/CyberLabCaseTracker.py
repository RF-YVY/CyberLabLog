
# IMPORTANT: Increment APP_VERSION and update RELEASE_DATE before each build for distribution!

# Example:
# APP_VERSION = "2.1.2"
# RELEASE_DATE = "Sep 3, 2025"

# --- Move Report Header Info Persistence into CaseLogApp ---


# --- Main Application Class ---

# (Moved report header info methods to the main CaseLogApp class below)


import ttkbootstrap as tb
from ttkbootstrap.constants import LEFT, RIGHT, TOP, BOTTOM, X, Y, BOTH, CENTER, W, E, N, S # Add/remove as needed
from ttkbootstrap.dialogs import Messagebox
from ttkbootstrap.widgets import DateEntry
import json
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog, scrolledtext
import sqlite3
import os
import io
import time
from datetime import datetime, date as datetime_date, timedelta # For isinstance check and date math
from PIL import Image, ImageTk
import shutil
import logging
import hashlib
import secrets
import sys
import random
import threading
import concurrent.futures
import queue
import calendar
import requests
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkintermapview
from tkintermapview import decimal_to_osm
from geopy.geocoders import Nominatim
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as ReportLabImage
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.pdfbase.pdfmetrics import stringWidth
import pandas as pd
from typing import Any
# --- App Constants & Paths ---
APP_NAME = "CyberLab Case Tracker"
APP_VERSION = "2.1.6"  # Increment before each build
RELEASE_DATE = "Oct 23, 2025"  # Update before each build
# Determine a persistent base directory:
# - When frozen by PyInstaller (--onefile), use the folder containing the executable
#   so data (DB, app_data) persists across runs.
# - When running from source, use the directory of this file.
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(os.path.abspath(sys.executable))
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, 'app_data')
DB_FILENAME = os.path.join(BASE_DIR, 'caselog_gui_v6.db')
LOG_FILENAME = os.path.join(DATA_DIR, 'app.log')
LOGO_FILENAME = os.path.join(DATA_DIR, 'logo.png')
MARKER_ICON_FILENAME = os.path.join(DATA_DIR, 'marker_icon.png')
ICON_FILENAME = os.path.join(BASE_DIR, 'digital.ico')
BACKUP_DIR = os.path.join(DATA_DIR, 'backups')
DEFAULT_PASSWORD = "password"

# Ensure persistent data directories exist early (for images, logs, backups)
try:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
except Exception:
    # Non-fatal; specific operations will re-attempt as needed
    pass

# If running from a PyInstaller bundle, seed default assets from the bundle on first run
try:
    if getattr(sys, 'frozen', False):
        bundle_dir = getattr(sys, '_MEIPASS', None)
        if bundle_dir:
            src_app_data = os.path.join(bundle_dir, 'app_data')
            if os.path.isdir(src_app_data):
                # copy logo.png and marker_icon.png if missing
                for fname in ('logo.png', 'marker_icon.png'):
                    dst = os.path.join(DATA_DIR, fname)
                    if not os.path.exists(dst):
                        src = os.path.join(src_app_data, fname)
                        if os.path.exists(src):
                            try:
                                shutil.copy2(src, dst)
                            except Exception:
                                pass
except Exception:
    pass

# Theme options exposed in Settings for ttkbootstrap
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
    ("Yeti", "yeti"),
]

# Common US state abbreviations (used in combos and map focal)
US_STATE_ABBREVIATIONS = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS",
    "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY",
    "NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV",
    "WI","WY","DC"
]

# Default map marker icon placeholder (PhotoImage) assigned at runtime
DEFAULT_MARKER_ICON = None

# Columns allowed to be updated in the case_log table when editing completed cases
CASE_LOG_MUTABLE_FIELDS = (
    "case_number",
    "examiner",
    "investigator",
    "agency",
    "city_of_offense",
    "state_of_offense",
    "start_date",
    "end_date",
    "volume_size_gb",
    "offense_type",
    "device_type",
    "forensic_tool",
    "model",
    "os",
    "data_recovered",
    "fpr_complete",
    "notes",
)

# Default options to seed editable combos when no history exists yet
EDITABLE_COMBO_DEFAULTS = {
    "forensic_tool": ["Cellebrite", "GrayKey"],
}

# --- Utilities: Backup and Window Icon ---
def perform_db_backup(retention_days: int = 56, keep_last: int | None = 5) -> str | None:
    """Create a timestamped SQLite DB backup and prune old backups.
    retention_days: delete backups older than this many days.
    keep_last: keep only the most recent N backups (after date-based prune).
    Returns the backup file path, or None on failure.
    """
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(DB_FILENAME))[0]
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_path = os.path.join(BACKUP_DIR, f"{base_name}_{ts}.db")

        # Prefer SQLite backup API; fallback to file copy
        try:
            src = sqlite3.connect(DB_FILENAME)
            dest = sqlite3.connect(backup_path)
            with dest:
                src.backup(dest)
            src.close()
            dest.close()
        except Exception as e:
            logging.warning(f"sqlite backup() failed ({e}); using copy2.")
            shutil.copy2(DB_FILENAME, backup_path)

        # Prune by age
        try:
            cutoff = datetime.now() - timedelta(days=retention_days)
            for fname in os.listdir(BACKUP_DIR):
                if not fname.lower().endswith('.db'):
                    continue
                if not fname.startswith(base_name + "_"):
                    continue
                fpath = os.path.join(BACKUP_DIR, fname)
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
                    if mtime < cutoff:
                        os.remove(fpath)
                        logging.info(f"Pruned old backup (age): {fpath}")
                except Exception as e:
                    logging.debug(f"Skip age prune {fpath}: {e}")
        except Exception as e:
            logging.debug(f"Age-based prune skipped: {e}")

        # Enforce keep-last count
        try:
            backups = [
                os.path.join(BACKUP_DIR, f)
                for f in os.listdir(BACKUP_DIR)
                if f.lower().endswith('.db') and f.startswith(base_name + "_")
            ]
            backups.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            if keep_last is not None and keep_last > 0 and len(backups) > keep_last:
                for old in backups[keep_last:]:
                    try:
                        os.remove(old)
                        logging.info(f"Pruned old backup (count): {old}")
                    except Exception as e:
                        logging.debug(f"Skip count prune {old}: {e}")
        except Exception as e:
            logging.debug(f"Count-based prune skipped: {e}")

        logging.info(f"Database backup created: {backup_path}")
        return backup_path
    except Exception as e:
        logging.error(f"Database backup failed: {e}")
        return None

def _set_window_icon(root):
    """Set the application window icon if available (Windows-friendly)."""
    try:
        icon_candidates = [ICON_FILENAME]
        bundle_dir = getattr(sys, "_MEIPASS", None)
        if bundle_dir:
            icon_candidates.append(os.path.join(bundle_dir, "digital.ico"))

        for candidate in icon_candidates:
            if not candidate or not os.path.exists(candidate):
                continue
            try:
                root.iconbitmap(candidate)
                if candidate != ICON_FILENAME:
                    try:
                        shutil.copy2(candidate, ICON_FILENAME)
                    except Exception:
                        pass
                return
            except Exception:
                continue
        # Fallback to logo.png as iconphoto if available
        if os.path.exists(LOGO_FILENAME):
            try:
                img = Image.open(LOGO_FILENAME)
                img = img.convert('RGBA')
                photo = ImageTk.PhotoImage(img)
                root.iconphoto(True, photo)
            except Exception:
                pass
    except Exception:
        pass

# Runtime geocache counters
GEOCACHE_HITS = 0
GEOCACHE_MISSES = 0

def init_db():
    """Initializes the SQLite database and creates the case_log table if it doesn't exist."""
    conn = None # Initialize conn to None
    try:
        db_path = os.path.abspath(DB_FILENAME)
        logging.info(f"[init_db] Using database file: {db_path}")
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=8000")
        except Exception:
            pass
        cursor = conn.cursor()

        # Create case_log table with an auto-incrementing primary key 'id'
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS case_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_number TEXT,
                examiner TEXT,
                offense_type TEXT,
                device_type TEXT,
                forensic_tool TEXT,
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

        # Create settings table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        # Check if password hash exists, if not, set default
        cursor.execute("SELECT value FROM settings WHERE key = 'password_hash'")
        if cursor.fetchone() is None:
            salt = generate_salt()
            hashed_password = hash_password(DEFAULT_PASSWORD, salt)
            cursor.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('password_hash', hashed_password))
            cursor.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('salt', salt)) # Store salt separately
            logging.info("Default password hash and salt set in settings.")

        # Create geocache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS geocache (
                location_key TEXT PRIMARY KEY, -- e.g., "City|State"
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                last_accessed TEXT 
            )
        ''')
        logging.info("Geocache table initialized or already exists.")

        # Create in_progress_cases table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS in_progress_cases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_number TEXT,
                examiner TEXT,
                offense_type TEXT,
                device_type TEXT,
                forensic_tool TEXT,
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
                created_at TEXT,
                priority TEXT DEFAULT 'Medium',
                target_due_date TEXT,
                progress_percentage INTEGER DEFAULT 0
            )
        ''')
        logging.info("In-progress cases table initialized or already exists.")

        # Remove activity logging table if present (feature deprecated)
        try:
            cursor.execute('DROP TABLE IF EXISTS case_activities')
            logging.info("Dropped deprecated case_activities table (if existed).")
        except Exception:
            pass

        # Add new columns to existing in_progress_cases table if they don't exist
        try:
            cursor.execute("ALTER TABLE case_log ADD COLUMN forensic_tool TEXT")
            logging.info("Added forensic_tool column to case_log table.")
        except sqlite3.OperationalError:
            pass  # Column already exists

        try:
            cursor.execute("ALTER TABLE in_progress_cases ADD COLUMN priority TEXT DEFAULT 'Medium'")
            logging.info("Added priority column to in_progress_cases table.")
        except sqlite3.OperationalError:
            pass  # Column already exists

        try:
            cursor.execute("ALTER TABLE in_progress_cases ADD COLUMN target_due_date TEXT")
            logging.info("Added target_due_date column to in_progress_cases table.")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Phase 3: Add workflow status column
        try:
            cursor.execute("ALTER TABLE in_progress_cases ADD COLUMN workflow_status TEXT DEFAULT 'Intake'")
            logging.info("Added workflow_status column to in_progress_cases table.")
        except sqlite3.OperationalError:
            pass  # Column already exists

        try:
            cursor.execute("ALTER TABLE in_progress_cases ADD COLUMN forensic_tool TEXT")
            logging.info("Ensured forensic_tool column exists on in_progress_cases table.")
        except sqlite3.OperationalError:
            pass

        conn.commit()
        # Create helpful indexes for common filters/queries (safe if already exist)
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_case_city_state ON case_log(city_of_offense, state_of_offense)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_case_offense ON case_log(offense_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_case_dates ON case_log(start_date, end_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ip_case_city_state ON in_progress_cases(city_of_offense, state_of_offense)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ip_case_offense ON in_progress_cases(offense_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ip_case_dates ON in_progress_cases(start_date, target_due_date)")
        except Exception as e:
            logging.warning(f"Index creation warning: {e}")
        conn.commit()
        logging.info("Database initialized successfully.")

    except sqlite3.Error as e:
        logging.error(f"Database error during initialization: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during database initialization: {e}")
    finally:
        if conn:
            conn.close()

def get_cached_location_db(location_key):
    """Retrieves cached latitude and longitude for a location_key."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=8000")
        except Exception:
            pass
        cursor = conn.cursor()
        cursor.execute("SELECT latitude, longitude FROM geocache WHERE location_key = ?", (location_key,))
        row = cursor.fetchone()
        if row:
            global GEOCACHE_HITS
            GEOCACHE_HITS += 1
            # Optionally, update last_accessed timestamp if you want to manage cache eviction later
            # timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # cursor.execute("UPDATE geocache SET last_accessed = ? WHERE location_key = ?", (timestamp, location_key))
            # conn.commit()
            logging.debug(f"Cache hit for location_key: {location_key}")
            return row[0], row[1]
        else:
            global GEOCACHE_MISSES
            GEOCACHE_MISSES += 1
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
        conn = sqlite3.connect(DB_FILENAME, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=8000")
        except Exception:
            pass
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
        
def optimize_database():
    """Run VACUUM and ANALYZE to optimize the SQLite database."""
    try:
        conn = sqlite3.connect(DB_FILENAME, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=8000")
        except Exception:
            pass
        cur = conn.cursor()
        cur.execute("PRAGMA optimize")
        cur.execute("VACUUM")
        cur.execute("ANALYZE")
        conn.commit()
    except Exception as e:
        logging.warning(f"DB optimize failed or partial: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_database_health_stats():
    """Return a dict with DB size, row counts, geocache entries, cache hits/misses, last backup path/time."""
    stats = {}
    try:
        stats['db_path'] = os.path.abspath(DB_FILENAME)
        stats['db_size_bytes'] = os.path.getsize(DB_FILENAME) if os.path.exists(DB_FILENAME) else 0
    except Exception:
        stats['db_size_bytes'] = 0
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM case_log")
        stats['case_log_rows'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM in_progress_cases")
        stats['in_progress_rows'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM geocache")
        stats['geocache_rows'] = cur.fetchone()[0]
    except Exception:
        stats['case_log_rows'] = stats.get('case_log_rows', 0)
        stats['in_progress_rows'] = stats.get('in_progress_rows', 0)
        stats['geocache_rows'] = stats.get('geocache_rows', 0)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    # Runtime counters
    try:
        stats['geocache_hits'] = GEOCACHE_HITS
        stats['geocache_misses'] = GEOCACHE_MISSES
    except Exception:
        stats['geocache_hits'] = 0
        stats['geocache_misses'] = 0
    # Backups
    try:
        if os.path.isdir(BACKUP_DIR):
            backups = [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.lower().endswith('.db')]
            backups.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            stats['last_backup'] = backups[0] if backups else None
        else:
            stats['last_backup'] = None
    except Exception:
        stats['last_backup'] = None
    return stats

def add_case_db(case_data):
    """Adds a new completed case to the database and logs a creation activity."""
    conn = None
    try:
        db_path = os.path.abspath(DB_FILENAME)
        logging.info(f"[add_case_db] Using database file: {db_path}")
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=8000")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
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
                start_date, end_date, volume_size_gb, offense_type, device_type, forensic_tool, model, os,
                data_recovered, fpr_complete, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            case_data.get("forensic_tool"),
            case_data.get("model"),
            case_data.get("os"),
            dr_str,
            fpr_int,
            case_data.get("notes"),
            created_at
        ))
        new_case_id = cursor.lastrowid
        # Commit the insert before logging (to avoid SQLite database locked error)
        conn.commit()

        # Log activity for completed cases timeline (after commit)
    # Activity timeline disabled: skip logging

        logging.info(f"Case '{case_data.get('case_number', '')}' added to database.")
        return True
    except Exception as e:
        logging.error(f"Error adding case '{case_data.get('case_number', 'N/A')}' to database: {e}")
        return False
    finally:
        if conn:
            conn.close()

def add_in_progress_case_db(case_data):
    """Adds a new case to the in_progress_cases database."""
    conn = None
    try:
        db_path = os.path.abspath(DB_FILENAME)
        logging.info(f"[add_in_progress_case_db] Using database file: {db_path}")
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
            INSERT INTO in_progress_cases (
                case_number, examiner, investigator, agency, city_of_offense, state_of_offense,
                start_date, end_date, volume_size_gb, offense_type, device_type, forensic_tool, model, os,
                data_recovered, fpr_complete, notes, created_at, priority, target_due_date, workflow_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            case_data.get("forensic_tool"),
            case_data.get("model"),
            case_data.get("os"),
            dr_str,
            fpr_int,
            case_data.get("notes"),
            created_at,
            case_data.get("priority", "Medium"),
            case_data.get("target_due_date"),
            case_data.get("workflow_status", "Intake")  # Phase 3: Default workflow status
        ))

        # Commit insert before logging activity to avoid SQLite locks
        case_id = cursor.lastrowid
        conn.commit()

    # Activity timeline disabled: skip logging
        logging.info(f"In-progress case '{case_data.get('case_number', '')}' added to database.")
        return True
    except Exception as e:
        logging.error(f"Error adding in-progress case '{case_data.get('case_number', 'N/A')}' to database: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_all_in_progress_cases_db():
    """Retrieves all in-progress cases from the database."""
    conn = None
    try:
        db_path = os.path.abspath(DB_FILENAME)
        logging.info(f"[get_all_in_progress_cases_db] Using database file: {db_path}")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # To access columns by name
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM in_progress_cases")
        rows = cursor.fetchall()
        # Convert rows to list of dictionaries
        return [dict(row) for row in rows]
    except Exception as e:
        logging.error(f"Error retrieving all in-progress cases from database: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_in_progress_case_by_id_db(case_id):
    """Retrieves a single in-progress case by its database ID."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        conn.row_factory = sqlite3.Row  # To access columns by name
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM in_progress_cases WHERE id = ?", (case_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logging.error(f"Error retrieving in-progress case by ID '{case_id}': {e}")
        return None
    finally:
        if conn:
            conn.close()

def update_in_progress_case_db(case_id, case_data):
    """Updates an existing in-progress case record in the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Ensure the record exists
        cursor.execute("SELECT 1 FROM in_progress_cases WHERE id = ?", (case_id,))
        if not cursor.fetchone():
            logging.error(f"No in-progress case found with ID {case_id}")
            return False

        # Build dynamic SET clause, excluding immutable fields
        fields_to_update = [f for f in case_data.keys() if f not in ["id", "created_at"]]
        if not fields_to_update:
            logging.warning(f"No valid fields to update for in-progress case ID {case_id}.")
            return False

        # Normalize certain fields
        if "fpr_complete" in fields_to_update:
            case_data["fpr_complete"] = 1 if case_data.get("fpr_complete") else 0
        if "data_recovered" in fields_to_update:
            dr_val = case_data.get("data_recovered")
            if dr_val is True or (isinstance(dr_val, str) and dr_val.lower() in ["yes", "true", "1"]):
                case_data["data_recovered"] = "Yes"
            elif dr_val is False or (isinstance(dr_val, str) and dr_val.lower() in ["no", "false", "0"]):
                case_data["data_recovered"] = "No"
            else:
                case_data["data_recovered"] = ""

        set_clause = ", ".join([f"{f} = ?" for f in fields_to_update])
        values = tuple(case_data[f] for f in fields_to_update) + (case_id,)

        cursor.execute(
            f"""
            UPDATE in_progress_cases
            SET {set_clause}
            WHERE id = ?
            """,
            values,
        )
        conn.commit()
        logging.info(f"In-progress case ID {case_id} updated successfully.")
        return True
    except Exception as e:
        logging.error(f"Error updating in-progress case ID {case_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def move_case_to_completed(case_id: int) -> bool:
    """Move a case from in_progress_cases to case_log by ID."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT case_number, examiner, investigator, agency,
                   city_of_offense, state_of_offense,
                   start_date, end_date, volume_size_gb,
                   offense_type, device_type, forensic_tool, model, os,
                   data_recovered, fpr_complete, notes, created_at
            FROM in_progress_cases WHERE id = ?
            """,
            (case_id,),
        )
        row = cursor.fetchone()
        if not row:
            logging.error(f"No in-progress case found with ID {case_id}")
            return False

        cursor.execute(
            """
            INSERT INTO case_log (
                case_number, examiner, investigator, agency, city_of_offense, state_of_offense,
                start_date, end_date, volume_size_gb, offense_type, device_type, forensic_tool, model, os,
                data_recovered, fpr_complete, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(row),
        )
        cursor.execute("DELETE FROM in_progress_cases WHERE id = ?", (case_id,))
        conn.commit()
        logging.info(f"Case ID {case_id} moved from in-progress to completed successfully.")
        return True
    except Exception as e:
        logging.error(f"Failed to move case ID {case_id} to completed: {e}")
        return False
    finally:
        if conn:
            conn.close()

def delete_in_progress_case_db(case_id: int) -> bool:
    """Delete an in-progress case by ID."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM in_progress_cases WHERE id = ?", (case_id,))
        conn.commit()
        logging.info(f"Deleted in-progress case ID {case_id}.")
        return cursor.rowcount > 0
    except Exception as e:
        logging.error(f"Error deleting in-progress case ID {case_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

    # Activity logging functions removed (feature deprecated)

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
    import time
    attempts = 0
    while attempts < 5:
        conn = None
        try:
            conn = sqlite3.connect(DB_FILENAME, timeout=10)
            try:
                conn.execute("PRAGMA busy_timeout=8000")
            except Exception:
                pass
            cursor = conn.cursor()

            # Limit updates to columns that exist on the case_log table to avoid SQL errors
            fields_to_update = [field for field in CASE_LOG_MUTABLE_FIELDS if field in case_data]
            set_clause = ', '.join([f'{field} = ?' for field in fields_to_update])
            if not set_clause:
                logging.warning(f"No valid fields to update for case ID {case_id}.")
                return False

            if 'fpr_complete' in fields_to_update:
                case_data['fpr_complete'] = 1 if case_data.get('fpr_complete') else 0
            if 'data_recovered' in fields_to_update:
                dr_val = case_data.get('data_recovered')
                if dr_val is True or (isinstance(dr_val, str) and dr_val.lower() in ['yes', 'true', '1']):
                    case_data['data_recovered'] = 'Yes'
                elif dr_val is False or (isinstance(dr_val, str) and dr_val.lower() in ['no', 'false', '0']):
                    case_data['data_recovered'] = 'No'
                else:
                    case_data['data_recovered'] = ''

            values = tuple(case_data[field] for field in fields_to_update) + (case_id,)
            cursor.execute(
                f"""
                UPDATE case_log
                SET {set_clause}
                WHERE id = ?
                """,
                values,
            )
            conn.commit()
            logging.info(f"Case ID {case_id} updated successfully in DB.")
            return True
        except sqlite3.OperationalError as e:
            if 'locked' in str(e).lower():
                attempts += 1
                time.sleep(0.2 * attempts)
                continue
            logging.error(f"OperationalError updating case ID {case_id}: {e}")
            return False
        except Exception as e:
            logging.error(f"Failed to update case ID {case_id} in DB: {e}")
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    logging.error(f"Failed to update case ID {case_id} after {attempts} attempts (locked).")
    return False

    


def delete_case_db(case_id):
    """Deletes a case record from the database by its ID."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=8000")
        except Exception:
            pass
        cursor = conn.cursor()
        
        # Debug: Log what we're trying to delete
        logging.info(f"Attempting to delete case with ID: {case_id} (type: {type(case_id)})")
        
    # Activity timeline disabled: skip pre-delete logging

        # Check if case exists before deletion
        cursor.execute("SELECT COUNT(*) FROM case_log WHERE id = ?", (case_id,))
        count_before = cursor.fetchone()[0]
        logging.info(f"Cases found with ID {case_id}: {count_before}")
        
        cursor.execute("DELETE FROM case_log WHERE id = ?", (case_id,))
        rows_affected = cursor.rowcount
        conn.commit()
        
        logging.info(f"Delete operation affected {rows_affected} rows")
        if rows_affected > 0:
            logging.info(f"Case ID {case_id} deleted successfully from DB.")
            return True
        else:
            logging.warning(f"No case found with ID {case_id} to delete")
            return False
    except Exception as e:
        logging.error(f"Failed to delete case ID {case_id} from DB: {e}")
        # show_error ("DB Error", f"Delete case failed for ID {case_id}: {e}"); # Avoid messagebox in helper
        return False
    finally:
        if conn:
            conn.close()

def get_recent_activities(limit=10):
    """Return a simple list of recent activities derived from case_log timestamps.
    Since the dedicated activity timeline is disabled, we approximate recent activity
    as the most recently created cases.

    Returns a list of dicts with keys: activity_type, case_number, timestamp.
    """
    items = []
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cur = conn.cursor()
        cur.execute(
            "SELECT case_number, created_at FROM case_log ORDER BY datetime(created_at) DESC LIMIT ?",
            (int(limit) if isinstance(limit, int) else 10,)
        )
        for row in cur.fetchall():
            items.append({
                'activity_type': 'Case',
                'case_number': row[0],
                'timestamp': row[1],
            })
    except Exception as e:
        logging.warning(f"get_recent_activities failed: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return items


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

def get_json_setting(key, default):
    """Fetch JSON-serialized value from settings table, else return default."""
    try:
        vals = get_combo_values_db(f"json_{key}")
        if vals and vals[0]:
            return json.loads(vals[0])
    except Exception:
        pass
    return default

def set_json_setting(key, obj):
    """Store JSON-serialized value into settings table."""
    try:
        set_combo_values_db(f"json_{key}", [json.dumps(obj)])
    except Exception:
        logging.warning(f"Failed to save json setting '{key}'")

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


def safe_float_conversion(value):
    """Safely convert a value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    try:
        # Convert to string first to handle various input types
        str_value = str(value).strip()
        if not str_value or str_value.lower() in ['', 'none', 'null', 'n/a']:
            return 0.0
        return float(str_value)
    except (ValueError, TypeError):
        # Log the problematic value for debugging
        logging.warning(f"Could not convert volume value '{value}' to float, using 0.0")
        return 0.0


def format_volume_for_display(volume_gb: float) -> str:
    """Return a human-friendly string for a volume in gigabytes (auto-convert to TB)."""
    try:
        vol = float(volume_gb)
    except (TypeError, ValueError):
        vol = 0.0
    if vol > 999:
        return f"{vol / 1024.0:.2f} TB"
    return f"{vol:.2f} GB"


# --- Main Application Class ---


class CaseLogApp:
    def create_in_progress_widgets(self):
        """Creates the widgets for the In Progress tab (Treeview, buttons, search/filter bar)."""
        container = ttk.Frame(self.in_progress_frame)
        container.grid(row=0, column=0, sticky='nsew')
        self.in_progress_frame.rowconfigure(0, weight=1)
        self.in_progress_frame.columnconfigure(0, weight=1)

        # --- Search/Filter Bar ---
        search_frame = ttk.Frame(container)
        search_frame.grid(row=0, column=0, sticky='ew', pady=(5, 0), padx=5)
        ttk.Label(search_frame, text="Search/Filter:").pack(side='left', padx=(0, 5))
        self.in_progress_search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.in_progress_search_var, width=30)
        search_entry.pack(side='left', padx=(0, 5))
        search_button = ttk.Button(search_frame, text="Apply", command=self.apply_in_progress_filter)
        search_button.pack(side='left')
        clear_button = ttk.Button(search_frame, text="Clear", command=self.clear_in_progress_filter)
        clear_button.pack(side='left', padx=(5, 0))
        # Priority filter
        ttk.Label(search_frame, text="Priority:").pack(side='left', padx=(10, 5))
        self.priority_filter_var = tk.StringVar(value='All')
        priority_combo = ttk.Combobox(search_frame, textvariable=self.priority_filter_var, values=['All', 'Critical', 'High', 'Medium', 'Low'], state='readonly', width=10)
        priority_combo.pack(side='left', padx=(0, 5))
        priority_combo.bind('<<ComboboxSelected>>', lambda e: self.apply_in_progress_filter())
        search_entry.bind('<Return>', lambda e: self.apply_in_progress_filter())

        # --- Button Bar ---
        button_frame = ttk.Frame(container)
        button_frame.grid(row=1, column=0, sticky='ew', pady=(0, 10), padx=5)
        refresh_button = ttk.Button(button_frame, text="Refresh", command=self.refresh_in_progress_view)
        refresh_button.pack(side='left', padx=(0, 5))
        bulk_priority_btn = ttk.Button(button_frame, text="Bulk Set Priority", command=self.bulk_set_priority)
        bulk_priority_btn.pack(side='left', padx=(0, 5))
        bulk_complete_btn = ttk.Button(button_frame, text="Bulk Complete", command=self.bulk_mark_completed, style="Accent.TButton")
        bulk_complete_btn.pack(side='left', padx=(0, 5))
        delete_button = ttk.Button(button_frame, text="Delete Selected", command=self.delete_selected_in_progress_cases, style="Danger.TButton")
        delete_button.pack(side='left', padx=(0, 5))
        mark_complete_btn = ttk.Button(button_frame, text="Mark as Completed", command=self.mark_case_as_completed)
        mark_complete_btn.pack(side='left', padx=(0, 5))

        # --- Treeview ---
        tree_frame = ttk.Frame(container)
        tree_frame.grid(row=2, column=0, sticky='nsew', padx=5, pady=5)
        container.rowconfigure(2, weight=1)
        container.columnconfigure(0, weight=1)

        self.in_progress_tree = ttk.Treeview(tree_frame, show='headings')
        self.in_progress_tree_columns_config = {
            'id': {'text': 'ID', 'width': 50},
            'priority': {'text': 'Priority', 'width': 80},
            'workflow_status': {'text': 'Status', 'width': 120},
            'case_number': {'text': 'Case #', 'width': 120},
            'examiner': {'text': 'Examiner', 'width': 120},
            'investigator': {'text': 'Investigator', 'width': 120},
            'agency': {'text': 'Agency', 'width': 120},
            'city_of_offense': {'text': 'City', 'width': 100},
            'state_of_offense': {'text': 'State', 'width': 60},
            'start_date': {'text': 'Start Date', 'width': 100},
            'end_date': {'text': 'End Date', 'width': 100},
            'target_due_date': {'text': 'Due Date', 'width': 100},
            'volume_size_gb': {'text': 'Volume (GB)', 'width': 100},
            'offense_type': {'text': 'Offense Type', 'width': 120},
            'device_type': {'text': 'Device Type', 'width': 100},
            'forensic_tool': {'text': 'Forensic Tool', 'width': 120},
            'model': {'text': 'Model', 'width': 100},
            'os': {'text': 'OS', 'width': 80},
            'data_recovered': {'text': 'Data Recovered?', 'width': 120},
            'fpr_complete': {'text': 'FPR Complete?', 'width': 120},
            'notes': {'text': 'Notes', 'width': 200},
            'created_at': {'text': 'Created', 'width': 150}
        }
        all_columns = list(self.in_progress_tree_columns_config.keys())
        self.in_progress_tree['columns'] = all_columns
        for col in all_columns:
            config = self.in_progress_tree_columns_config[col]
            self.in_progress_tree.heading(col, text=config['text'])
            if col == 'id':
                self.in_progress_tree.column(col, width=0, minwidth=0, stretch=False)
                self.in_progress_tree.heading(col, text='')
            else:
                self.in_progress_tree.column(col, width=config['width'], minwidth=50)

        # Scrollbars
        v_scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.in_progress_tree.yview)
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        self.in_progress_tree.configure(yscrollcommand=v_scrollbar.set)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient='horizontal', command=self.in_progress_tree.xview)
        h_scrollbar.grid(row=1, column=0, sticky='ew')
        self.in_progress_tree.configure(xscrollcommand=h_scrollbar.set)

        self.in_progress_tree.grid(row=0, column=0, sticky='nsew')
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        # Bind events
        self.in_progress_tree.bind('<Double-1>', lambda e: self.edit_selected_in_progress_case())
        self.in_progress_tree.bind('<Return>', lambda e: self.edit_selected_in_progress_case())
        self.in_progress_tree.bind('<Delete>', lambda e: self.delete_selected_in_progress_cases())

        # Initial data load
        self.refresh_in_progress_view()
    def check_for_updates(self, silent=False):
        """Check GitHub for a newer release version."""
        import threading, requests
        def do_check():
            try:
                url = "https://api.github.com/repos/RF-YVY/CyberLabLog/releases/latest"
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    latest_version = data.get('tag_name') or data.get('name')
                    if latest_version and latest_version.lstrip('v') > APP_VERSION:
                        msg = f"A new version ({latest_version}) is available on GitHub.\nVisit https://github.com/RF-YVY/CyberLabLog to download."
                        self.root.after(0, lambda: messagebox.showinfo("Update Available", msg))
                    elif not silent:
                        self.root.after(0, lambda: messagebox.showinfo("No Update", "You are running the latest version."))
                elif not silent:
                    self.root.after(0, lambda: messagebox.showwarning("Update Check Failed", "Could not check for updates (GitHub API error)."))
            except Exception as e:
                if not silent:
                    self.root.after(0, lambda: messagebox.showwarning("Update Check Failed", f"Could not check for updates: {e}"))
        threading.Thread(target=do_check, daemon=True).start()
    # Lazy-loading page size for the View Data Treeview
    LAZY_PAGE_SIZE = 200
    def _show_report_saved_dialog(self, filename: str, tone: str = "snarky"):
        """Show a confirmation dialog that a report was saved, with rotating personalities.
        tone: 'professional' | 'friendly' | 'snarky' | 'fun' | 'random'
        Default is 'snarky' to match the snarky messagebox mode.
        """
        try:
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
            bucket = tone if tone in messages else (random.choice(list(messages.keys())) if tone == "random" else "professional")
            title = {
                "professional": "Report Saved",
                "friendly": "Nice!",
                "snarky": "Saved (Obviously)",
                "fun": "Success!",
            }[bucket]
            body_intro = random.choice(messages[bucket])
            Messagebox.show_info(title, f"{body_intro}\n\nSaved to:\n{filename}")
        except Exception:
            # Fallback to a simple info box
            try:
                Messagebox.show_info("Report Saved", f"Saved to:\n{filename}")
            except Exception:
                pass
    # --- Theme contrast helpers ---
    def _get_current_theme_code(self) -> str:
        try:
            if hasattr(self.root, 'style') and hasattr(self.root.style, 'theme'):
                return getattr(self.root.style.theme, 'name', '') or ''
        except Exception:
            pass
        try:
            return getattr(self, '_saved_theme_code', '') or ''
        except Exception:
            return ''

    def _get_contrast_fg(self) -> str:
        """Return '#000000' or '#ffffff' depending on current theme darkness."""
        code = (self._get_current_theme_code() or '').lower()
        dark_codes = {
            'cyborg','darkly','slate','solar','superhero','vapor','morph'
        }
        try:
            return '#ffffff' if code in dark_codes else '#000000'
        except Exception:
            return '#000000'

    def refresh_contrast_colors(self):
        """Apply contrast-aware foreground to summary numeric labels."""
        try:
            fg = self._get_contrast_fg()
            if hasattr(self, '_total_cases_value_label') and self._total_cases_value_label:
                try:
                    self._total_cases_value_label.config(foreground=fg)
                except Exception:
                    pass
            if hasattr(self, '_total_volume_value_label') and self._total_volume_value_label:
                try:
                    self._total_volume_value_label.config(foreground=fg)
                except Exception:
                    pass
        except Exception:
            pass
    # --- Editable Combobox Management (register, refresh, manage) ---
    def _init_combo_registry(self):
        """Initialize registry of editable combobox widgets by key."""
        try:
            from collections import defaultdict
            self._combo_registry = defaultdict(list)
        except Exception:
            self._combo_registry = {}
        # Keys whose options are user-managed
        self._editable_combo_keys = [
            "examiner", "investigator", "agency", "offense_type", "city_of_offense", "forensic_tool"
        ]

    def _register_editable_combo(self, key: str, combo: ttk.Combobox, var: tk.StringVar):
        """Track a combobox for live updates and attach context menu bindings."""
        try:
            if not hasattr(self, '_combo_registry'):
                self._init_combo_registry()
            if key in self._editable_combo_keys:
                widget_list = self._combo_registry.get(key, [])
                if combo not in widget_list:
                    widget_list.append(combo)
                    self._combo_registry[key] = widget_list
                # Right-click context menu
                def show_menu(event, k=key, cb=combo, v=var):
                    menu = tk.Menu(cb, tearoff=0)
                    current_val = (v.get() or '').strip()
                    if current_val:
                        menu.add_command(label=f"Add '{current_val}' to list", command=lambda: self._add_value_to_combo_list(k, current_val))
                        menu.add_command(label=f"Delete '{current_val}' from list", command=lambda: self._delete_value_from_combo_list(k, current_val))
                    else:
                        menu.add_command(label="Add current value…", state='disabled')
                        menu.add_command(label="Delete current value…", state='disabled')
                    menu.add_separator()
                    menu.add_command(label="Manage List…", command=lambda: self._open_manage_combo_dialog(k))
                    try:
                        menu.tk_popup(event.x_root, event.y_root)
                    finally:
                        menu.grab_release()
                combo.bind('<Button-3>', show_menu)  # Windows right-click
                combo.bind('<Menu>', show_menu)       # Keyboard context menu key
        except Exception as e:
            logging.warning(f"Failed to register editable combo for '{key}': {e}")

    def _refresh_registered_combos(self, key: str, values: list):
        """Update the 'values' list for all registered combos of a key."""
        try:
            widgets = self._combo_registry.get(key, []) if hasattr(self, '_combo_registry') else []
            for cb in list(widgets):
                try:
                    cb['values'] = values
                except Exception:
                    # Prune dead widgets
                    try:
                        widgets.remove(cb)
                    except Exception:
                        pass
        except Exception as e:
            logging.debug(f"Combo refresh skipped for '{key}': {e}")

    def _get_initial_combo_values(self, key: str) -> list:
        """Union of persisted values and values derived from existing cases, de-duplicated and sorted."""
        try:
            persisted = get_combo_values_db(key) or []
        except Exception:
            persisted = []
        derived = []
        try:
            derived = get_unique_field_values(key) or []
        except Exception:
            pass
        try:
            merged_set = {v for v in (persisted + derived) if isinstance(v, str)}
        except Exception:
            merged_set = set(persisted or derived or [])
        # Seed with defaults when available so users always see helpful starting options
        defaults = EDITABLE_COMBO_DEFAULTS.get(key, [])
        merged_set.update(defaults)
        merged = sorted(merged_set)
        # Persist back if registry exists (first run hydration)
        try:
            set_combo_values_db(key, merged)
        except Exception:
            pass
        return merged

    def _add_value_to_combo_list(self, key: str, value: str):
        value = (value or '').strip()
        if not value:
            return
        try:
            values = get_combo_values_db(key)
            if value not in values:
                values.append(value)
                # Keep sorted for stable UX
                try:
                    values = sorted(values)
                except Exception:
                    pass
                set_combo_values_db(key, values)
                self._refresh_registered_combos(key, values)
        except Exception as e:
            logging.warning(f"Could not add '{value}' to list '{key}': {e}")

    def _delete_value_from_combo_list(self, key: str, value: str):
        value = (value or '').strip()
        if not value:
            return
        try:
            values = get_combo_values_db(key)
            if value in values:
                values = [v for v in values if v != value]
                set_combo_values_db(key, values)
                self._refresh_registered_combos(key, values)
        except Exception as e:
            logging.warning(f"Could not delete '{value}' from list '{key}': {e}")

    def _open_manage_combo_dialog(self, key: str):
        """Dialog to view/delete values for an editable combo key."""
        try:
            win = tk.Toplevel(self.root)
            win.title(f"Manage List — {key.replace('_', ' ').title()}")
            win.geometry("360x360")
            win.grab_set()
            frm = ttk.Frame(win, padding=10)
            frm.pack(fill='both', expand=True)
            lb = tk.Listbox(frm)
            vals = get_combo_values_db(key)
            for v in vals:
                lb.insert('end', v)
            lb.pack(fill='both', expand=True)
            btns = ttk.Frame(frm)
            btns.pack(fill='x', pady=(8,0))
            def do_delete():
                sel = list(lb.curselection())
                if not sel:
                    return
                # Delete from end to start to keep indices valid
                targets = [lb.get(i) for i in sel]
                new_vals = [v for v in get_combo_values_db(key) if v not in targets]
                set_combo_values_db(key, new_vals)
                # Refresh listbox and all registered combos
                lb.delete(0, 'end')
                for v in new_vals:
                    lb.insert('end', v)
                self._refresh_registered_combos(key, new_vals)
            def do_close():
                win.destroy()
            ttk.Button(btns, text="Delete Selected", command=do_delete).pack(side='left')
            ttk.Button(btns, text="Close", command=do_close).pack(side='right')
        except Exception as e:
            logging.error(f"Failed to open Manage List dialog for '{key}': {e}")
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
            location = geolocator.geocode(f"{state}, USA")
            if location:
                lat = getattr(location, "latitude", None)
                lon = getattr(location, "longitude", None)
                if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                    self.map_widget.set_position(float(lat), float(lon))
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

        # Button row
        btn_row = ttk.Frame(win)
        btn_row.grid(row=len(fields), column=0, columnspan=2, pady=(10, 8))

        def do_save():
            data = {f: v.get().strip() for f, v in vars.items()}
            try:
                self.set_report_header_info(data)
                try:
                    Messagebox.show_info("Saved", "Report header info saved.")
                except Exception:
                    pass
                win.destroy()
            except Exception as e:
                try:
                    Messagebox.show_error("Save Failed", f"Could not save header info: {e}")
                except Exception:
                    pass

        ttk.Button(btn_row, text="Save", command=do_save).pack(side='right', padx=(6, 0))
        ttk.Button(btn_row, text="Cancel", command=win.destroy).pack(side='right')

    # --- Lazy Loading for View Data Treeview ---
    def init_lazy_loading(self):
        """Initialize lazy loading state for the View Data Treeview."""
        self._lazy_offset = 0
        self._lazy_total = 0
        self._lazy_cases = []
        self._lazy_filter = None
        self._lazy_loading = False

    def refresh_data_view(self, filter_text=None, reset_lazy=True):
        """Refresh the Treeview with lazy loading support."""
        if not hasattr(self, 'tree') or self.tree is None:
            return
        # Re-apply visible columns instantly after user changes column selection
        visible_columns = self.get_visible_treeview_columns()
        try:
            self.tree.configure(displaycolumns=visible_columns)
        except Exception:
            pass
        # Use existing filter string if none provided
        if filter_text is None:
            filter_text = getattr(self, '_view_filter_string', '') if hasattr(self, '_view_filter_string') else ''

        if reset_lazy:
            self.init_lazy_loading()
            self._lazy_filter = filter_text
            self._lazy_cases = self.get_filtered_cases(filter_text)
            self._lazy_total = len(self._lazy_cases)
            self._lazy_offset = 0
        try:
            self.tree.delete(*self.tree.get_children())
        except Exception:
            pass
        self.load_next_lazy_page()

    def get_filtered_cases(self, filter_text):
        """Return filtered cases for the current filter/search text."""
        try:
            all_cases = get_all_cases_db()
        except Exception:
            all_cases = []
        if not filter_text:
            return all_cases
        ft = str(filter_text).lower().strip()
        filtered = []
        for case in all_cases:
            try:
                for v in case.values():
                    if v and ft in str(v).lower():
                        filtered.append(case)
                        break
            except Exception:
                continue
        return filtered

    def load_next_lazy_page(self):
        """Load the next page of cases into the Treeview."""
        if not hasattr(self, 'tree') or self.tree is None:
            return
        if getattr(self, '_lazy_loading', False):
            return
        self._lazy_loading = True
        try:
            start = getattr(self, '_lazy_offset', 0)
            total = getattr(self, '_lazy_total', 0)
            end = min(start + self.LAZY_PAGE_SIZE, total)
            cases = getattr(self, '_lazy_cases', [])[start:end]
            # Ensure columns are set
            for case in cases:
                values = [case.get(col, "") for col in self.tree["columns"]]
                self.tree.insert("", "end", values=values)
            self._lazy_offset = end
        finally:
            self._lazy_loading = False

    def on_treeview_scroll(self, *args):
        """Callback for Treeview vertical scroll. Loads more data if near bottom."""
        self.tree.yview(*args)
        # Check if near bottom
        if self.tree.yview()[1] > 0.95 and self._lazy_offset < self._lazy_total:
            self.load_next_lazy_page()
    
    def show_date_range_report(self):
        """Show dialog for date range report with option to include in-progress cases."""
        from tkinter import Toplevel, Label, Button, StringVar, ttk, messagebox
        from datetime import datetime, timedelta
        import calendar as _cal

        win = Toplevel(self.root)
        win.title("Date Range Report")
        win.geometry("520x380")
        try:
            win.minsize(500, 360)
        except Exception:
            pass
        win.grab_set()

        # Centered content wrapper
        center = ttk.Frame(win)
        center.pack(fill='both', expand=True)
        content = ttk.Frame(center)
        content.pack(anchor='center', padx=10, pady=10)

        # Date range selection
        Label(content, text="Date Range Report", font=('TkDefaultFont', 12, 'bold')).grid(row=0, column=0, columnspan=3, pady=(10, 15))

        Label(content, text="Start Date:").grid(row=1, column=0, sticky='w', padx=10, pady=5)
        start_var = StringVar(value="")
        start_entry = DateEntry(content, dateformat='%Y-%m-%d', firstweekday=0, bootstyle="secondary")
        start_entry.grid(row=1, column=1, padx=5, pady=5, sticky='w')
        try:
            start_entry.entry.configure(textvariable=start_var)
        except Exception:
            pass

        Label(content, text="End Date:").grid(row=2, column=0, sticky='w', padx=10, pady=5)
        end_var = StringVar(value="")
        end_entry = DateEntry(content, dateformat='%Y-%m-%d', firstweekday=0, bootstyle="secondary")
        end_entry.grid(row=2, column=1, padx=5, pady=5, sticky='w')
        try:
            end_entry.entry.configure(textvariable=end_var)
        except Exception:
            pass

        # Quick date selections (single row)
        quick_row = ttk.Frame(content)
        quick_row.grid(row=3, column=0, columnspan=3, pady=(6, 10), sticky='w')
        Label(quick_row, text="Quick:").pack(side='left', padx=(0, 6))

        def set_last_30_days():
            today = datetime.now().date()
            start = today - timedelta(days=30)
            sv = start.strftime('%Y-%m-%d')
            ev = today.strftime('%Y-%m-%d')
            start_var.set(sv)
            end_var.set(ev)
            try:
                start_entry.entry.delete(0, 'end'); start_entry.entry.insert(0, sv)
                end_entry.entry.delete(0, 'end'); end_entry.entry.insert(0, ev)
            except Exception:
                pass

        def set_current_month():
            today = datetime.now().date()
            first = today.replace(day=1)
            last_day = _cal.monthrange(today.year, today.month)[1]
            last = today.replace(day=last_day)
            sv = first.strftime('%Y-%m-%d')
            ev = last.strftime('%Y-%m-%d')
            start_var.set(sv)
            end_var.set(ev)
            try:
                start_entry.entry.delete(0, 'end'); start_entry.entry.insert(0, sv)
                end_entry.entry.delete(0, 'end'); end_entry.entry.insert(0, ev)
            except Exception:
                pass

        ttk.Button(quick_row, text="Last 30 Days", command=set_last_30_days).pack(side='left', padx=4)
        ttk.Button(quick_row, text="Current Month", command=set_current_month).pack(side='left', padx=4)

        # Data source selection
        src_row = ttk.LabelFrame(content, text="Data Source")
        src_row.grid(row=4, column=0, columnspan=3, padx=10, pady=(0, 10), sticky='w')
        src_var = StringVar(value="completed")
        ttk.Radiobutton(src_row, text="Completed", value="completed", variable=src_var).pack(side='left', padx=(6, 8), pady=4)
        ttk.Radiobutton(src_row, text="In-Progress Only", value="inprogress", variable=src_var).pack(side='left', padx=8, pady=4)
        ttk.Radiobutton(src_row, text="Both", value="both", variable=src_var).pack(side='left', padx=8, pady=4)

        # Output selection
        out_row = ttk.LabelFrame(content, text="Output")
        out_row.grid(row=5, column=0, columnspan=3, padx=10, pady=(0, 10), sticky='w')
        out_var = StringVar(value="pdf")
        ttk.Radiobutton(out_row, text="PDF (save)", value="pdf", variable=out_var).pack(side='left', padx=(6, 8), pady=4)
        ttk.Radiobutton(out_row, text="Preview (Text + PDF export)", value="preview", variable=out_var).pack(side='left', padx=8, pady=4)

        def generate_date_range_report():
            # Validate dates
            sv = (start_var.get() or "").strip()
            ev = (end_var.get() or "").strip()
            if not sv or not ev:
                try:
                    messagebox.showerror("Invalid Dates", "Please select both start and end dates.")
                except Exception:
                    pass
                return
            try:
                sd = datetime.strptime(sv[:10], '%Y-%m-%d')
                ed = datetime.strptime(ev[:10], '%Y-%m-%d')
            except Exception:
                try:
                    messagebox.showerror("Invalid Dates", "Dates must be in YYYY-MM-DD format.")
                except Exception:
                    pass
                return
            # Fetch and filter
            selected = src_var.get()
            completed_cases = []
            try:
                for c in get_all_cases_db() or []:
                    d_raw = c.get('start_date') or c.get('created_at')
                    if not d_raw:
                        continue
                    try:
                        dt = datetime.strptime(str(d_raw)[:10], '%Y-%m-%d')
                    except Exception:
                        continue
                    if sd <= dt <= ed:
                        completed_cases.append(c)
            except Exception:
                completed_cases = []

            inprog_cases = []
            try:
                for c in get_all_in_progress_cases_db() or []:
                    d_raw = c.get('created_at')
                    if not d_raw:
                        continue
                    try:
                        dt = datetime.strptime(str(d_raw)[:10], '%Y-%m-%d')
                    except Exception:
                        continue
                    if sd <= dt <= ed:
                        inprog_cases.append(c)
            except Exception:
                inprog_cases = []

            # Choose dataset(s)
            if selected == "completed":
                cases_for_view = completed_cases
                inprog_for_view = None
            elif selected == "inprogress":
                cases_for_view = []
                inprog_for_view = inprog_cases
            else:
                cases_for_view = completed_cases
                inprog_for_view = inprog_cases

            if not cases_for_view and not inprog_for_view:
                Messagebox.show_info("Date Range Report", "No cases found in the selected range.")
                return

            # Output action
            if out_var.get() == "pdf":
                try:
                    self.export_date_range_report_pdf(cases_for_view, sv, ev, in_progress_cases=inprog_for_view)
                finally:
                    try:
                        win.destroy()
                    except Exception:
                        pass
            else:
                # Open preview (text) with export button
                self.generate_date_range_summary(cases_for_view, sv, ev, in_progress_cases=inprog_for_view)

        # Action button
        Button(content, text="Generate Report", command=generate_date_range_report, bg='lightblue').grid(row=6, column=0, columnspan=3, pady=12)
    
    def build_date_range_text_report(self, cases, start_date, end_date, in_progress_cases=None):
        """Build and return plain-text content for the date range report.
        If in_progress_cases is provided, totals cover both sets and an extra section is added.
        """
        from datetime import datetime
        report_content = "DATE RANGE REPORT\n"
        report_content += f"Period: {start_date} to {end_date}\n"
        report_content += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        report_content += "=" * 60 + "\n\n"

        in_progress_cases = in_progress_cases or []
        all_cases = list(cases) + list(in_progress_cases)
        total_cases = len(all_cases)
        try:
            total_gb = sum(safe_float_conversion(c.get('volume_size_gb')) for c in all_cases)
        except Exception:
            total_gb = 0.0
        total_tb = total_gb / 1024 if total_gb > 999 else None

        report_content += "SUMMARY STATISTICS\n"
        report_content += f"Total Devices: {total_cases}\n"
        if total_tb:
            report_content += f"Total Volume: {total_tb:.2f} TB\n\n"
        else:
            report_content += f"Total Volume: {total_gb:.2f} GB\n\n"

        examiners, agencies, offense_types, forensic_tools = {}, {}, {}, {}
        for case in all_cases:
            examiner = case.get('examiner', 'Unknown')
            agency = case.get('agency', 'Unknown')
            offense = case.get('offense_type', 'Unknown')
            tool = case.get('forensic_tool', 'Unknown') or 'Unknown'
            examiners[examiner] = examiners.get(examiner, 0) + 1
            agencies[agency] = agencies.get(agency, 0) + 1
            offense_types[offense] = offense_types.get(offense, 0) + 1
            forensic_tools[tool] = forensic_tools.get(tool, 0) + 1

        report_content += "CASES BY EXAMINER\n"
        report_content += "-" * 30 + "\n"
        for examiner, count in sorted(examiners.items()):
            report_content += f"{examiner}: {count}\n"

        report_content += "\nCASES BY AGENCY\n"
        report_content += "-" * 30 + "\n"
        for agency, count in sorted(agencies.items()):
            report_content += f"{agency}: {count}\n"

        report_content += "\nCASES BY OFFENSE TYPE\n"
        report_content += "-" * 30 + "\n"
        for offense, count in sorted(offense_types.items()):
            report_content += f"{offense}: {count}\n"

        report_content += "\nCASES BY FORENSIC TOOL\n"
        report_content += "-" * 30 + "\n"
        for tool, count in sorted(forensic_tools.items()):
            report_content += f"{tool}: {count}\n"

        report_content += "\nCOMPLETED CASE DETAILS\n"
        report_content += "-" * 30 + "\n"
        for i, case in enumerate(cases, 1):
            report_content += f"{i}. Case #{case.get('case_number', 'N/A')} - "
            report_content += f"{case.get('examiner', 'N/A')} - "
            report_content += f"{case.get('start_date', 'N/A')}"
            tool = case.get('forensic_tool')
            if tool:
                report_content += f" - Tool: {tool}"
            report_content += "\n"

        if in_progress_cases:
            report_content += "\nIN-PROGRESS CASE DETAILS\n"
            report_content += "-" * 30 + "\n"
            for i, case in enumerate(in_progress_cases, 1):
                report_content += f"{i}. Case #{case.get('case_number', 'N/A')} - "
                report_content += f"{case.get('examiner', 'N/A')} - "
                report_content += f"{case.get('created_at', 'N/A')}"
                tool = case.get('forensic_tool')
                if tool:
                    report_content += f" - Tool: {tool}"
                report_content += "\n"

        return report_content

    def generate_date_range_summary(self, cases, start_date, end_date, orientation="Auto", in_progress_cases=None):
        """Generate and display a summary report for the given date range."""
        from tkinter import Toplevel, Text, Scrollbar

        # Create summary window
        summary_win = Toplevel(self.root)
        summary_win.title(f"Date Range Report: {start_date} to {end_date}")
        summary_win.geometry("600x500")
        try:
            summary_win.minsize(560, 420)
        except Exception:
            pass

        # Content area: text widget with scrollbar
        text_frame = ttk.Frame(summary_win)
        text_frame.pack(side='top', fill='both', expand=True, padx=10, pady=10)

        text_widget = Text(text_frame, wrap='word')
        scrollbar = Scrollbar(text_frame, orient='vertical', command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)

        text_widget.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        # Generate report content via helper
        report_content = self.build_date_range_text_report(cases, start_date, end_date, in_progress_cases=in_progress_cases)
        # Insert content
        text_widget.insert('1.0', report_content)
        text_widget.config(state='disabled')  # Make read-only

        # Bottom action bar: keep buttons visible
        btn_bar = ttk.Frame(summary_win)
        btn_bar.pack(side='bottom', fill='x', padx=10, pady=(0, 10))

        close_btn = ttk.Button(btn_bar, text="Close", command=summary_win.destroy)
        close_btn.pack(side='right')

        export_btn = ttk.Button(
            btn_bar,
            text="Export to PDF",
            command=lambda: self.export_date_range_report_pdf(cases, start_date, end_date, in_progress_cases=in_progress_cases)
        )
        export_btn.pack(side='right', padx=(0,10))
    
    def export_date_range_report(self, content, start_date, end_date):
        """Export date range report to a text file."""
        from tkinter import filedialog
        import os
        
        try:
            filename = filedialog.asksaveasfilename(
                defaultextension=".txt",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
                initialfile=f"date_range_report_{start_date}_to_{end_date}.txt"
            )
            
            if filename:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                messagebox.showinfo("Success", f"Date range report exported to:\n{filename}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export report: {e}")

    def export_date_range_report_pdf(self, cases, start_date, end_date, page_size="Letter", orientation="Portrait", in_progress_cases=None):
        """Export the date range report to PDF with dynamic widths and optional in-progress section (no forced page breaks)."""
        from tkinter import filedialog
        import os
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.pagesizes import letter, legal, A4, landscape, portrait
        from reportlab.lib import colors
        from reportlab.lib.units import inch

        filename = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf")],
            title="Save Date Range Report (PDF)"
        )
        if not filename:
            return

        size_map = {"Letter": letter, "Legal": legal, "A4": A4}
        base_size = size_map.get(page_size, letter)
        pagesize = portrait(base_size) if orientation != "Landscape" else landscape(base_size)
        headers = ["#", "Case #", "Start", "Examiner", "Agency", "Offense", "Device", "Forensic Tool"]

        doc = SimpleDocTemplate(filename, pagesize=pagesize, rightMargin=20, leftMargin=20, topMargin=24, bottomMargin=24)
        page_width = pagesize[0] - doc.leftMargin - doc.rightMargin
        styles = getSampleStyleSheet()
        elements = []

        # Header block
        header_info = self.get_report_header_info()
        header_lines = [
            f"Name: {header_info.get('Name','')}",
            f"Agency: {header_info.get('Agency','')}",
            f"Division: {header_info.get('Division','')}",
            f"Date: {header_info.get('Date','')}"
        ]
        elements.append(Table([[Paragraph(line, styles["Normal"])] for line in header_lines], hAlign='LEFT'))
        elements.append(Spacer(1, 10))

        # Title + logo
        try:
            title_para = Paragraph(f"<b>Date Range Report ({start_date} to {end_date})</b>", styles["Title"]) 
            if os.path.exists(LOGO_FILENAME):
                logo_w = 1.0 * inch
                img = RLImage(LOGO_FILENAME, width=logo_w, height=logo_w)
                title_tbl = Table([[title_para, img]], colWidths=[None, logo_w])
                title_tbl.setStyle(TableStyle([
                    ("ALIGN", (0,0), (0,0), "LEFT"),
                    ("ALIGN", (1,0), (1,0), "RIGHT"),
                    ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                ]))
                elements.append(title_tbl)
            else:
                elements.append(title_para)
        except Exception:
            elements.append(Paragraph("<b>Date Range Report</b>", styles["Title"]))
        elements.append(Spacer(1, 12))

        # Summary
        in_progress_cases = in_progress_cases or []
        all_cases = list(cases) + list(in_progress_cases)
        total_cases = len(all_cases)
        try:
            total_gb = sum(safe_float_conversion(c.get('volume_size_gb')) for c in all_cases)
        except Exception:
            total_gb = 0.0
        total_tb = total_gb / 1024 if total_gb > 999 else None
        elements.append(Paragraph("<b>Summary Statistics</b>", styles["Heading2"]))
        elements.append(Paragraph(f"Total Devices: {total_cases}", styles["Normal"]))
        elements.append(Paragraph(f"Total Volume: {total_tb:.2f} TB" if total_tb else f"Total Volume: {total_gb:.2f} GB", styles["Normal"]))
        elements.append(Spacer(1, 8))

        def group_counts(items, key):
            d = {}
            for c in items:
                v = c.get(key, 'Unknown') or 'Unknown'
                d[v] = d.get(v, 0) + 1
            return sorted(d.items(), key=lambda kv: (-kv[1], str(kv[0]).lower()))

        for key, label in (("examiner", "Cases by Examiner"), ("agency", "Cases by Agency"), ("offense_type", "Cases by Offense Type"), ("forensic_tool", "Cases by Forensic Tool")):
            pairs = group_counts(all_cases, key)
            if not pairs:
                continue
            cnt_w = 1.2 * inch
            val_w = max(1.5 * inch, page_width - cnt_w)
            tbl = Table([["Value", "Count"]] + [[k, v] for k, v in pairs], colWidths=[val_w, cnt_w])
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
                ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
                ("ALIGN", (1,1), (1,-1), "RIGHT"),
                ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
                ("FONTSIZE", (0,0), (-1,-1), 9),
            ]))
            elements.append(Paragraph(f"<b>{label}</b>", styles["Heading3"]))
            elements.append(tbl)
            elements.append(Spacer(1, 8))

        # Completed details (no forced page break)
        elements.append(Paragraph("<b>Completed Case Details</b>", styles["Heading2"]))
        rows = [headers]
        for i, c in enumerate(cases, 1):
            rows.append([
                str(i),
                c.get('case_number', ''),
                format_date_str_for_display(c.get('start_date') or c.get('created_at', '')),
                c.get('examiner', ''),
                c.get('agency', ''),
                c.get('offense_type', ''),
                c.get('device_type', ''),
                c.get('forensic_tool', ''),
            ])

        min_w = 0.4 * inch
        max_w = 2.0 * inch
        weights = []
        for ci in range(len(headers)):
            max_len = len(str(rows[0][ci]))
            for r in rows[1:]:
                val = '' if ci >= len(r) or r[ci] is None else str(r[ci])
                if len(val) > max_len:
                    max_len = len(val)
            weights.append(max(0.6, min(3.0, max_len / 10)))
        tw = sum(weights) or 1.0
        widths = [(w / tw) * page_width for w in weights]
        col_widths = [max(min_w, min(max_w, w)) for w in widths]
        extra = page_width - sum(col_widths)
        if extra > 0:
            j = max(range(len(col_widths)), key=lambda i: col_widths[i])
            col_widths[j] += extra
        tbl_completed = Table(rows, colWidths=col_widths, repeatRows=1, splitByRow=True)
        tbl_completed.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("FONTSIZE", (0,0), (-1,-1), 8),
            ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ]))
        elements.append(tbl_completed)

        # In-Progress details (no forced page break)
        if in_progress_cases:
            elements.append(Paragraph("<b>In-Progress Case Details</b>", styles["Heading2"]))
            ip_headers = ["#", "Case #", "Created", "Examiner", "Agency", "Offense", "Device", "Forensic Tool", "Priority", "Workflow", "Target Due"]
            ip_rows = [ip_headers]
            for i, c in enumerate(in_progress_cases, 1):
                ip_rows.append([
                    str(i),
                    c.get('case_number', ''),
                    format_date_str_for_display(c.get('created_at', '')),
                    c.get('examiner', ''),
                    c.get('agency', ''),
                    c.get('offense_type', ''),
                    c.get('device_type', ''),
                    c.get('forensic_tool', ''),
                    c.get('priority', ''),
                    c.get('workflow_status', ''),
                    format_date_str_for_display(c.get('target_due_date', '')),
                ])

            min_w = 0.4 * inch
            max_w = 2.2 * inch
            weights = []
            for ci in range(len(ip_headers)):
                max_len = len(str(ip_rows[0][ci]))
                for r in ip_rows[1:]:
                    val = '' if ci >= len(r) or r[ci] is None else str(r[ci])
                    if len(val) > max_len:
                        max_len = len(val)
                weights.append(max(0.6, min(3.0, max_len / 10)))
            tw = sum(weights) or 1.0
            widths = [(w / tw) * page_width for w in weights]
            ip_col_widths = [max(min_w, min(max_w, w)) for w in widths]
            extra = page_width - sum(ip_col_widths)
            if extra > 0:
                j = max(range(len(ip_col_widths)), key=lambda i: ip_col_widths[i])
                ip_col_widths[j] += extra
            tbl_ip = Table(ip_rows, colWidths=ip_col_widths, repeatRows=1, splitByRow=True)
            tbl_ip.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
                ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
                ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
                ("FONTSIZE", (0,0), (-1,-1), 8),
                ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
            ]))
            elements.append(tbl_ip)

        try:
            doc.build(elements)
            self._show_report_saved_dialog(filename)
        except Exception as e:
            Messagebox.show_error("Date Range Report", f"Failed to generate PDF: {e}")

    def show_total_case_summary(self):
        """Show dialog for total case summary options and generate a summary report."""
        import tkinter as tk
        from tkinter import Toplevel, Label, Button, StringVar, IntVar, Checkbutton
        from datetime import datetime, timedelta

        win = Toplevel(self.root)
        win.title("Total Case Summary Options")
        win.grab_set()

        # Date range
        Label(win, text="Start Date (YYYY-MM-DD):").grid(row=0, column=0, sticky='w', padx=10, pady=(10,2))
        start_var = StringVar(value="")
        start_entry = tk.Entry(win, textvariable=start_var, width=12)
        start_entry.grid(row=0, column=1, padx=5, pady=(10,2))
        Label(win, text="End Date (YYYY-MM-DD):").grid(row=1, column=0, sticky='w', padx=10, pady=2)
        end_var = StringVar(value="")
        end_entry = tk.Entry(win, textvariable=end_var, width=12)
        end_entry.grid(row=1, column=1, padx=5, pady=2)

        # Recent activity
        recent_var = IntVar(value=0)
        Label(win, text="Recent Activity (days):").grid(row=2, column=0, sticky='w', padx=10, pady=2)
        recent_days_var = StringVar(value="7")
        recent_entry = tk.Entry(win, textvariable=recent_days_var, width=5)
        recent_entry.grid(row=2, column=1, padx=5, pady=2)
        recent_check = Checkbutton(win, text="Show only recent cases", variable=recent_var)
        recent_check.grid(row=2, column=2, padx=5, pady=2)

        # Output format
        Label(win, text="Output format:").grid(row=3, column=0, sticky='w', padx=10, pady=(10,2))
        fmt_var = StringVar(value="PDF")
        Button(win, text="PDF", command=lambda: fmt_var.set("PDF")).grid(row=3, column=1, sticky='w')
        Button(win, text="Excel (XLSX)", command=lambda: fmt_var.set("XLSX")).grid(row=3, column=2, sticky='w')

        # Page size and orientation for PDF
        Label(win, text="Page Size:").grid(row=4, column=0, sticky='w', padx=10, pady=(10,2))
        page_size_var = StringVar(value="Letter")
        page_size_combo = ttk.Combobox(win, textvariable=page_size_var, values=["Letter", "Legal", "A4"], state='readonly', width=12)
        page_size_combo.grid(row=4, column=1, sticky='w', padx=5, pady=(10,2))

        Label(win, text="Orientation:").grid(row=5, column=0, sticky='w', padx=10, pady=2)
        orientation_var = StringVar(value="Auto")
        orientation_combo = ttk.Combobox(win, textvariable=orientation_var, values=["Auto", "Portrait", "Landscape"], state='readonly', width=12)
        orientation_combo.grid(row=5, column=1, sticky='w', padx=5, pady=2)

        def do_summary():
            # Parse date range
            start_date = start_var.get().strip()
            end_date = end_var.get().strip()
            cases = get_all_cases_db()
            filtered = []
            for c in cases:
                created = c.get('created_at') or c.get('start_date')
                if created:
                    try:
                        dt = datetime.strptime(str(created)[:10], '%Y-%m-%d')
                    except Exception:
                        continue
                    if start_date:
                        try:
                            sd = datetime.strptime(start_date, '%Y-%m-%d')
                            if dt < sd:
                                continue
                        except Exception:
                            pass
                    if end_date:
                        try:
                            ed = datetime.strptime(end_date, '%Y-%m-%d')
                            if dt > ed:
                                continue
                        except Exception:
                            pass
                    filtered.append(c)

            # Recent activity filter
            if recent_var.get():
                try:
                    days = int(recent_days_var.get())
                    cutoff = datetime.now() - timedelta(days=days)
                    filtered = [c for c in filtered if c.get('created_at') and datetime.strptime(str(c['created_at'])[:10], '%Y-%m-%d') >= cutoff]
                except Exception:
                    pass

            if not filtered:
                Messagebox.show_info("Summary", "No cases found for the selected range/criteria.")
                return

            if fmt_var.get() == "PDF":
                self.export_total_case_summary_pdf(
                    filtered,
                    start_date,
                    end_date,
                    recent_var.get(),
                    recent_days_var.get(),
                    page_size=page_size_var.get(),
                    orientation=orientation_var.get(),
                )
            else:
                self.export_total_case_summary_xlsx(filtered, start_date, end_date, recent_var.get(), recent_days_var.get())
            win.destroy()

        Button(win, text="Generate Summary", command=do_summary).grid(row=6, column=0, columnspan=3, pady=15)

    def show_all_cases_summary(self):
        """Generate overall summary using all cases without date filtering."""
        import tkinter as tk
        from tkinter import Toplevel, Label, Button, StringVar

        win = Toplevel(self.root)
        win.title("All Cases Summary")
        win.grab_set()

        # Configure grid for better spacing
        try:
            for c in range(0, 4):
                win.grid_columnconfigure(c, weight=1)
        except Exception:
            pass

        # Export format (radiobuttons)
        fmt_group = ttk.LabelFrame(win, text="Export Format")
        fmt_group.grid(row=0, column=0, columnspan=4, padx=10, pady=(10,5), sticky='we')
        fmt_var = StringVar(value="PDF")
        ttk.Radiobutton(fmt_group, text="PDF", variable=fmt_var, value="PDF").pack(side='left', padx=10, pady=6)
        ttk.Radiobutton(fmt_group, text="Excel (XLSX)", variable=fmt_var, value="XLSX").pack(side='left', padx=10, pady=6)

        # Data source selection (Completed / In-Progress / Both)
        src_group = ttk.LabelFrame(win, text="Data Source")
        src_group.grid(row=1, column=0, columnspan=4, padx=10, pady=(5,5), sticky='we')
        src_var = StringVar(value="completed")
        ttk.Radiobutton(src_group, text="Completed", variable=src_var, value="completed").pack(side='left', padx=10, pady=6)
        ttk.Radiobutton(src_group, text="In-Progress Only", variable=src_var, value="inprogress").pack(side='left', padx=10, pady=6)
        ttk.Radiobutton(src_group, text="Both", variable=src_var, value="both").pack(side='left', padx=10, pady=6)

        # Page size and orientation for PDF
        Label(win, text="Page Size:").grid(row=2, column=0, sticky='w', padx=10, pady=(5,2))
        page_size_var = StringVar(value="Letter")
        page_size_combo = ttk.Combobox(win, textvariable=page_size_var, values=["Letter", "Legal", "A4"], state='readonly', width=12)
        page_size_combo.grid(row=2, column=1, sticky='w', padx=5, pady=(5,2))

        Label(win, text="Orientation:").grid(row=3, column=0, sticky='w', padx=10, pady=2)
        orientation_var = StringVar(value="Auto")
        orientation_combo = ttk.Combobox(win, textvariable=orientation_var, values=["Auto", "Portrait", "Landscape"], state='readonly', width=12)
        orientation_combo.grid(row=3, column=1, sticky='w', padx=5, pady=2)

        def do_export():
            # Determine data sources
            completed_cases = get_all_cases_db() or []
            inprog_cases = get_all_in_progress_cases_db() or []
            selected = src_var.get()
            if selected == "completed":
                cases = completed_cases
                ip_cases = []
            elif selected == "inprogress":
                cases = []
                ip_cases = inprog_cases
            else:
                cases = completed_cases
                ip_cases = inprog_cases

            if not cases and not ip_cases:
                Messagebox.show_info("Summary", "No cases available.")
                return

            if fmt_var.get() == "PDF":
                self.export_all_cases_summary_pdf(
                    cases,
                    in_progress_cases=ip_cases,
                    page_size=page_size_var.get(),
                    orientation=orientation_var.get(),
                )
            else:
                combined = list(cases) + list(ip_cases)
                self.export_total_case_summary_xlsx(combined, "", "", 0, "")
            win.destroy()

        Button(win, text="Generate Summary", command=do_export).grid(row=4, column=0, columnspan=4, padx=10, pady=12, sticky='we')

    def export_total_case_summary_pdf(self, cases, start_date, end_date, recent_only, recent_days, page_size="Letter", orientation="Auto"):
        from tkinter import filedialog
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.pagesizes import letter, legal, A4, landscape, portrait
        from reportlab.lib import colors
        from reportlab.lib.units import inch
        filename = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF files", "*.pdf")], title="Save Total Summary PDF")
        if not filename:
            return
        # Resolve page size and orientation
        size_map = {"Letter": letter, "Legal": legal, "A4": A4}
        base_size = size_map.get(page_size, letter)
        # Auto: landscape when recent table shown (5 cols), otherwise portrait (2-col breakdowns fit fine)
        use_landscape = True if orientation == "Landscape" else False
        if orientation == "Auto":
            use_landscape = True if recent_only else False
        pagesize = landscape(base_size) if use_landscape else portrait(base_size)

        doc = SimpleDocTemplate(filename, pagesize=pagesize,
                                 leftMargin=20, rightMargin=20, topMargin=24, bottomMargin=24)
        elements = []
        styles = getSampleStyleSheet()
        page_width = pagesize[0] - doc.leftMargin - doc.rightMargin
        # Header info (top left)
        header_info = self.get_report_header_info()
        header_lines = [
            f"Name: {header_info.get('Name','')}",
            f"Agency: {header_info.get('Agency','')}",
            f"Division: {header_info.get('Division','')}",
            f"Date: {header_info.get('Date','')}"
        ]
        header_table = Table([[Paragraph(line, styles["Normal"])] for line in header_lines], hAlign='LEFT')
        elements.append(header_table)
        elements.append(Spacer(1, 12))
        # Logo and title (top right)
        try:
            if os.path.exists(LOGO_FILENAME):
                from PIL import Image as PILImage
                pil_img = PILImage.open(LOGO_FILENAME)
                orig_w, orig_h = pil_img.size
                max_dim = 1.1 * inch
                if orig_w > orig_h:
                    logo_width = max_dim
                    logo_height = max_dim * (orig_h / orig_w)
                else:
                    logo_height = max_dim
                    logo_width = max_dim * (orig_w / orig_h)
                img = RLImage(LOGO_FILENAME, width=logo_width, height=logo_height)
                title = "Total Case Summary"
                if start_date or end_date:
                    title += f" ({start_date or '...'} to {end_date or '...'})"
                if recent_only:
                    title += f" (Recent {recent_days} days)"
                # Reduce title font size and allow word wrap
                from reportlab.lib.styles import ParagraphStyle
                small_title_style = ParagraphStyle('SmallTitle', parent=styles["Title"], fontSize=14, leading=16, alignment=1, wordWrap='CJK')
                title_para = Paragraph(f"<b>{title}</b>", small_title_style)
                logo_table = Table(
                    [[title_para, img]],
                    colWidths=[None, logo_width],
                )
                logo_table.setStyle(TableStyle([
                    ("ALIGN", (0,0), (0,0), "LEFT"),
                    ("ALIGN", (1,0), (1,0), "RIGHT"),
                    ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                    ("LEFTPADDING", (1,0), (1,0), 6),
                    ("RIGHTPADDING", (1,0), (1,0), 0),
                    ("TOPPADDING", (0,0), (-1,-1), 2),
                    ("BOTTOMPADDING", (0,0), (-1,-1), 2),
                ]))
                elements.append(logo_table)
                elements.append(Spacer(1, 12))
            else:
                title = "Total Case Summary"
                if start_date or end_date:
                    title += f" ({start_date or '...'} to {end_date or '...'})"
                if recent_only:
                    title += f" (Recent {recent_days} days)"
                from reportlab.lib.styles import ParagraphStyle
                small_title_style = ParagraphStyle('SmallTitle', parent=styles["Title"], fontSize=14, leading=16, alignment=1, wordWrap='CJK')
                elements.append(Paragraph(f"<b>{title}</b>", small_title_style))
                elements.append(Spacer(1, 12))
        except Exception:
            title = "Total Case Summary"
            if start_date or end_date:
                title += f" ({start_date or '...'} to {end_date or '...'})"
            if recent_only:
                title += f" (Recent {recent_days} days)"
            elements.append(Paragraph(f"<b>{title}</b>", styles["Title"]))
            elements.append(Spacer(1, 12))
        # Totals
        total_cases = len(cases)
        total_gb = sum(safe_float_conversion(c.get('volume_size_gb')) for c in cases)
        total_tb = total_gb / 1024 if total_gb > 999 else None
        elements.append(Paragraph(f"<b>Total Devices:</b> {total_cases}", styles["Normal"]))
        if total_tb:
            elements.append(Paragraph(f"<b>Total Volume:</b> {total_tb:.2f} TB", styles["Normal"]))
        else:
            elements.append(Paragraph(f"<b>Total Volume:</b> {total_gb:.2f} GB", styles["Normal"]))

        # Benchmarking: average turnaround times per agency/tool and aging alerts
        from datetime import datetime as dt

        def _parse_iso_date(value):
            if not value:
                return None
            text = str(value).strip()
            if not text:
                return None
            candidates = [
                (text[:10], '%Y-%m-%d'),
                (text[:19], '%Y-%m-%d %H:%M:%S'),
                (text[:10], '%m-%d-%Y'),
            ]
            for sample, fmt in candidates:
                try:
                    return dt.strptime(sample, fmt).date()
                except ValueError:
                    continue
            return None

        def _accumulate_turnaround(dataset, field):
            stats = {}
            for case in dataset:
                start = _parse_iso_date(case.get('start_date'))
                end = _parse_iso_date(case.get('end_date'))
                if not start or not end or end < start:
                    continue
                elapsed = (end - start).days
                key = (case.get(field) or 'Unknown').strip() or 'Unknown'
                bucket = stats.setdefault(key, {'total': 0, 'count': 0})
                bucket['total'] += elapsed
                bucket['count'] += 1
            return stats

        turnaround_sections = [
            ("Average Turnaround by Agency", _accumulate_turnaround(cases, 'agency')),
            ("Average Turnaround by Forensic Tool", _accumulate_turnaround(cases, 'forensic_tool')),
        ]

        benchmarks_added = False
        for heading, stats in turnaround_sections:
            if not stats:
                continue
            if not benchmarks_added:
                elements.append(Spacer(1, 8))
                elements.append(Paragraph("<b>Benchmarking</b>", styles["Heading2"]))
                benchmarks_added = True
            rows = [["Value", "Avg Days", "Cases"]]
            sorted_items = sorted(
                stats.items(),
                key=lambda item: ((item[1]['total'] / item[1]['count']) if item[1]['count'] else float('inf'), item[0].lower())
            )
            for key, data in sorted_items:
                avg_days = data['total'] / data['count'] if data['count'] else 0
                rows.append([key, f"{avg_days:.1f}", str(data['count'])])
            table = Table(rows, colWidths=[max(2.0 * inch, page_width * 0.45), 1.0 * inch, 0.9 * inch])
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            elements.append(Paragraph(f"<b>{heading}</b>", styles["Heading3"]))
            elements.append(table)
            elements.append(Spacer(1, 6))

        # Case aging alerts (in-progress cases approaching or past due date)
        aging_threshold_days = 7
        aging_entries = []
        today = dt.now().date()
        try:
            active_cases = get_all_in_progress_cases_db() or []
        except Exception:
            active_cases = []
        for case in active_cases:
            due = _parse_iso_date(case.get('target_due_date'))
            if not due:
                continue
            delta = (due - today).days
            if delta < 0:
                status = "Overdue"
                days_display = str(abs(delta))
            elif delta == 0:
                status = "Due Today"
                days_display = "0"
            elif delta <= aging_threshold_days:
                status = "Due Soon"
                days_display = str(delta)
            else:
                continue
            aging_entries.append({
                'case': case.get('case_number', ''),
                'examiner': case.get('examiner', ''),
                'agency': case.get('agency', ''),
                'due': format_date_str_for_display(case.get('target_due_date', '')),
                'status': status,
                'days': days_display,
                'priority': case.get('priority', ''),
                'tool': case.get('forensic_tool', ''),
                'sort': delta,
            })

        if aging_entries:
            status_order = {"Overdue": 0, "Due Today": 1, "Due Soon": 2}
            aging_entries.sort(key=lambda entry: (status_order.get(entry['status'], 99), entry['sort']))
            table_rows = [["Case #", "Examiner", "Agency", "Due Date", "Status", "Days", "Priority", "Forensic Tool"]]
            table_rows.extend([
                [entry['case'], entry['examiner'], entry['agency'], entry['due'], entry['status'], entry['days'], entry['priority'], entry['tool']]
                for entry in aging_entries
            ])
            elements.append(Spacer(1, 8))
            elements.append(Paragraph("<b>Case Aging Alerts (Next 7 Days / Overdue)</b>", styles["Heading2"]))
            col_widths = [0.9 * inch, 1.2 * inch, 1.2 * inch, 1.0 * inch, 1.0 * inch, 0.8 * inch, 0.9 * inch, 1.3 * inch]
            aging_table = Table(table_rows, colWidths=col_widths, repeatRows=1)
            aging_table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (5, 1), (5, -1), "CENTER"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTSIZE", (0, 0), (-1, -1), 8.5),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            elements.append(aging_table)
            elements.append(Spacer(1, 10))

        # Breakdown by fields (dynamic widths)
        def breakdown(field):
            d = {}
            for c in cases:
                v = (c.get(field) or '').strip()
                if v:
                    d[v] = d.get(v, 0) + 1
            return sorted(d.items(), key=lambda x: x[1], reverse=True)
        for field, label in [
            ("examiner", "Examiner"),
            ("agency", "Agency"),
            ("offense_type", "Offense Type"),
            ("device_type", "Device Type"),
            ("forensic_tool", "Forensic Tool"),
        ]:
            items = breakdown(field)
            if items:
                elements.append(Spacer(1, 8))
                elements.append(Paragraph(f"<b>{label} Breakdown:</b>", styles["Normal"]))
                # Allocate a fixed count column width and give the rest to the value column
                count_col_w = 1.2 * inch
                value_col_w = max(1.5 * inch, page_width - count_col_w)
                t = Table([[k, v] for k, v in items], colWidths=[value_col_w, count_col_w])
                t.setStyle(TableStyle([
                    ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
                    ("ALIGN", (0,0), (-1,-1), "LEFT"),
                    ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
                    ("FONTSIZE", (0,0), (-1,-1), 9),
                    ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
                    ("ALIGN", (1,0), (1,-1), "RIGHT"),
                ]))
                elements.append(t)
        # List of recent cases (dynamic widths + repeat header)
        if recent_only:
            elements.append(Spacer(1, 12))
            elements.append(Paragraph(f"<b>Recent Cases (last {recent_days} days):</b>", styles["Normal"]))
            headers = ["Case #", "Created", "Examiner", "Offense", "Forensic Tool", "Vol (GB)"]
            rows = []
            for c in cases:
                rows.append([
                    c.get('case_number', ''),
                    format_date_str_for_display(c.get('created_at', '')),
                    c.get('examiner', ''),
                    c.get('offense_type', ''),
                    c.get('forensic_tool', ''),
                    c.get('volume_size_gb', ''),
                ])

            # Compute dynamic widths based on content lengths
            min_w = 0.6 * inch
            max_w = (2.6 if use_landscape else 2.0) * inch
            col_weights = []
            for ci in range(len(headers)):
                max_len = len(str(headers[ci]))
                for r in rows:
                    val = '' if ci >= len(r) or r[ci] is None else str(r[ci])
                    if len(val) > max_len:
                        max_len = len(val)
                # Heuristic: scale by character length
                col_weights.append(max(0.6, min(3.0, max_len / 10)))
            total_weight = sum(col_weights) or 1.0
            raw_widths = [(w / total_weight) * page_width for w in col_weights]
            col_widths = [max(min_w, min(max_w, w)) for w in raw_widths]
            # If we still have space, give it to the widest column
            total_w = sum(col_widths)
            if total_w < page_width:
                gap = page_width - total_w
                widest_idx = max(range(len(col_widths)), key=lambda i: col_widths[i])
                col_widths[widest_idx] += gap

            # Use smaller font and word wrap for headers
            from reportlab.lib.styles import ParagraphStyle
            header_style = ParagraphStyle('HeaderSmall', fontName='Helvetica-Bold', fontSize=8, leading=9, alignment=1, wordWrap='CJK')
            wrapped_headers = [Paragraph(h, header_style) for h in headers]
            t = Table([wrapped_headers] + rows, colWidths=col_widths, repeatRows=1, splitByRow=True)
            t.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
                ("ALIGN", (0,0), (-1,-1), "LEFT"),
                ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
                ("FONTSIZE", (0,0), (-1,-1), 8),
                ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
                ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
            ]))
            elements.append(t)
        doc.build(elements)
        self._show_report_saved_dialog(filename)

    def export_total_case_summary_xlsx(self, cases, start_date, end_date, recent_only, recent_days):
        from tkinter import filedialog
        import pandas as pd
        filename = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")], title="Save Total Summary Excel")
        if not filename:
            return
        # Build summary data
        total_cases = len(cases)
        total_gb = sum(safe_float_conversion(c.get('volume_size_gb')) for c in cases)
        total_tb = total_gb / 1024 if total_gb > 999 else None
        summary = {
            "Total Devices": [total_cases],
            "Total Volume (GB)": [total_gb],
            "Total Volume (TB)": [total_tb if total_tb else '']
        }
        df_summary = pd.DataFrame(summary)
        # Breakdown sheets
        def breakdown(field):
            d = {}
            for c in cases:
                v = (c.get(field) or '').strip()
                if v:
                    d[v] = d.get(v, 0) + 1
            return sorted(d.items(), key=lambda x: x[1], reverse=True)
        with pd.ExcelWriter(filename) as writer:
            df_summary.to_excel(writer, sheet_name="Summary", index=False)
            for field, label in [
                ("examiner", "Examiner"),
                ("agency", "Agency"),
                ("offense_type", "Offense Type"),
                ("device_type", "Device Type"),
                ("forensic_tool", "Forensic Tool"),
            ]:
                items = breakdown(field)
                if items:
                    df = pd.DataFrame(items, columns=[label, "Count"])
                    df.to_excel(writer, sheet_name=label, index=False)
            # Recent cases sheet
            if recent_only:
                rows = []
                for c in cases:
                    rows.append({
                        "Case #": c.get('case_number', ''),
                        "Created": format_date_str_for_display(c.get('created_at', '')),
                        "Examiner": c.get('examiner', ''),
                        "Offense": c.get('offense_type', ''),
                        "Forensic Tool": c.get('forensic_tool', ''),
                        "Vol (GB)": c.get('volume_size_gb', '')
                    })
                df_recent = pd.DataFrame(rows)
                df_recent.to_excel(writer, sheet_name=f"Recent_{recent_days}d", index=False)
        self._show_report_saved_dialog(filename)

    def export_all_cases_summary_pdf(self, completed_cases, in_progress_cases=None, page_size="Letter", orientation="Auto"):
        from tkinter import filedialog
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.pagesizes import letter, legal, A4, landscape, portrait
        from reportlab.lib import colors
        from reportlab.lib.units import inch
        import os

        in_progress_cases = in_progress_cases or []
        filename = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF files", "*.pdf")], title="Save All Cases Summary PDF")
        if not filename:
            return

        size_map = {"Letter": letter, "Legal": legal, "A4": A4}
        base_size = size_map.get(page_size, letter)
        # Use landscape if many columns (when including in-progress details), otherwise portrait
        use_landscape = True if orientation == "Landscape" else False
        if orientation == "Auto":
            use_landscape = True if in_progress_cases else False
        pagesize = landscape(base_size) if use_landscape else portrait(base_size)

        doc = SimpleDocTemplate(filename, pagesize=pagesize, leftMargin=20, rightMargin=20, topMargin=24, bottomMargin=24)
        styles = getSampleStyleSheet()
        page_width = pagesize[0] - doc.leftMargin - doc.rightMargin
        elements = []

        # Header
        header_info = self.get_report_header_info()
        header_lines = [
            f"Name: {header_info.get('Name','')}",
            f"Agency: {header_info.get('Agency','')}",
            f"Division: {header_info.get('Division','')}",
            f"Date: {header_info.get('Date','')}"
        ]
        header_table = Table([[Paragraph(line, styles["Normal"])] for line in header_lines])
        elements.append(header_table)
        elements.append(Spacer(1, 10))

        # Title with optional logo
        try:
            title = "All Cases Summary"
            title_para = Paragraph(f"<b>{title}</b>", styles["Title"]) 
            if os.path.exists(LOGO_FILENAME):
                logo_width = 1.1 * inch
                img = RLImage(LOGO_FILENAME, width=logo_width, height=logo_width)
                title_table = Table([[title_para, img]], colWidths=[None, logo_width])
                title_table.setStyle(TableStyle([
                    ("ALIGN", (0,0), (0,0), "LEFT"),
                    ("ALIGN", (1,0), (1,0), "RIGHT"),
                    ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                ]))
                elements.append(title_table)
            else:
                elements.append(title_para)
        except Exception:
            elements.append(Paragraph("<b>All Cases Summary</b>", styles["Title"]))
        elements.append(Spacer(1, 12))

        # Totals across both datasets
        all_cases = list(completed_cases) + list(in_progress_cases)
        total_cases = len(all_cases)
        total_gb = sum(safe_float_conversion(c.get('volume_size_gb')) for c in all_cases)
        total_tb = total_gb / 1024 if total_gb > 999 else None
        elements.append(Paragraph(f"<b>Total Devices:</b> {total_cases}", styles["Normal"]))
        vol_text = f"{total_tb:.2f} TB" if total_tb else f"{total_gb:.2f} GB"
        elements.append(Paragraph(f"<b>Total Volume:</b> {vol_text}", styles["Normal"]))
        elements.append(Spacer(1, 8))

        # Breakdown tables
        def breakdown(items, field):
            d = {}
            for c in items:
                v = (c.get(field) or '').strip() or 'Unknown'
                d[v] = d.get(v, 0) + 1
            return sorted(d.items(), key=lambda kv: (-kv[1], kv[0].lower()))

        for field, label in [
            ("examiner", "Examiner"),
            ("agency", "Agency"),
            ("offense_type", "Offense Type"),
            ("device_type", "Device Type"),
            ("forensic_tool", "Forensic Tool"),
        ]:
            pairs = breakdown(all_cases, field)
            if not pairs:
                continue
            count_col_w = 1.2 * inch
            value_col_w = max(1.5 * inch, page_width - count_col_w)
            t = Table([["Value", "Count"]] + [[k, v] for k, v in pairs], colWidths=[value_col_w, count_col_w])
            t.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
                ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
                ("ALIGN", (1,1), (1,-1), "RIGHT"),
                ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
                ("FONTSIZE", (0,0), (-1,-1), 9),
            ]))
            elements.append(Paragraph(f"<b>{label} Breakdown</b>", styles["Heading3"]))
            elements.append(t)
            elements.append(Spacer(1, 8))

        # Completed details
        headers = ["#", "Case #", "Created", "Examiner", "Agency", "Offense", "Device", "Forensic Tool"]
        rows = [headers]
        for i, c in enumerate(completed_cases, 1):
            rows.append([
                str(i),
                c.get('case_number', ''),
                format_date_str_for_display(c.get('start_date') or c.get('created_at', '')),
                c.get('examiner', ''),
                c.get('agency', ''),
                c.get('offense_type', ''),
                c.get('device_type', ''),
                c.get('forensic_tool', ''),
            ])
        elements.append(Paragraph("<b>Completed Case Details</b>", styles["Heading2"]))
        # width calc
        min_w = 0.4 * inch
        max_w = (2.5 if use_landscape else 2.0) * inch
        col_weights = []
        for ci in range(len(headers)):
            max_len = len(str(rows[0][ci]))
            for r in rows[1:]:
                val = '' if ci >= len(r) or r[ci] is None else str(r[ci])
                if len(val) > max_len:
                    max_len = len(val)
            col_weights.append(max(0.6, min(3.0, max_len / 10)))
        total_weight = sum(col_weights) or 1.0
        raw_widths = [(w / total_weight) * page_width for w in col_weights]
        col_widths = [max(min_w, min(max_w, w)) for w in raw_widths]
        total_w = sum(col_widths)
        if total_w < page_width:
            gap = page_width - total_w
            widest_idx = max(range(len(col_widths)), key=lambda i: col_widths[i])
            col_widths[widest_idx] += gap
        t_completed = Table(rows, colWidths=col_widths, repeatRows=1, splitByRow=True)
        t_completed.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("FONTSIZE", (0,0), (-1,-1), 8),
            ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ]))
        elements.append(t_completed)

        # In-progress details
        if in_progress_cases:
            ip_headers = [
                "#",
                "Case #",
                "Created",
                "Examiner",
                "Agency",
                "Offense",
                "Device",
                "Forensic Tool",
                "Priority",
                "Workflow",
                "Target Due",
            ]
            ip_rows = [ip_headers]
            for i, c in enumerate(in_progress_cases, 1):
                ip_rows.append([
                    str(i),
                    c.get('case_number', ''),
                    format_date_str_for_display(c.get('created_at', '')),
                    c.get('examiner', ''),
                    c.get('agency', ''),
                    c.get('offense_type', ''),
                    c.get('device_type', ''),
                    c.get('forensic_tool', ''),
                    c.get('priority', ''),
                    c.get('workflow_status', ''),
                    format_date_str_for_display(c.get('target_due_date', '')),
                ])
            elements.append(Paragraph("<b>In-Progress Case Details</b>", styles["Heading2"]))
            min_w = 0.4 * inch
            max_w = 2.2 * inch
            col_weights = []
            for ci in range(len(ip_headers)):
                max_len = len(str(ip_rows[0][ci]))
                for r in ip_rows[1:]:
                    val = '' if ci >= len(r) or r[ci] is None else str(r[ci])
                    if len(val) > max_len:
                        max_len = len(val)
                col_weights.append(max(0.6, min(3.0, max_len / 10)))
            total_weight = sum(col_weights) or 1.0
            raw_widths = [(w / total_weight) * page_width for w in col_weights]
            ip_col_widths = [max(min_w, min(max_w, w)) for w in raw_widths]
            total_w = sum(ip_col_widths)
            if total_w < page_width:
                gap = page_width - total_w
                widest_idx = max(range(len(ip_col_widths)), key=lambda i: ip_col_widths[i])
                ip_col_widths[widest_idx] += gap
            t_ip = Table(ip_rows, colWidths=ip_col_widths, repeatRows=1, splitByRow=True)
            t_ip.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
                ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
                ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
                ("FONTSIZE", (0,0), (-1,-1), 8),
                ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
            ]))
            elements.append(t_ip)

        doc.build(elements)
        self._show_report_saved_dialog(filename)
    def show_case_summary_report(self):
        """Generate a one-page PDF summary for the selected case."""
        from tkinter import filedialog
        if not self.tree.selection() or len(self.tree.selection()) != 1:
            Messagebox.show_info("Case Summary", "Please select exactly one case in the table.")
            return
        selected_id = self.tree.item(self.tree.selection()[0])['values'][0]
        case = get_case_by_id_db(selected_id)
        if not case:
            Messagebox.show_error("Case Summary", "Could not retrieve case details.")
            return
        # Ask for save location
        filename = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF files", "*.pdf")], title="Save Case Summary PDF")
        if not filename:
            return
        # Build PDF
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.lib.units import inch
        doc = SimpleDocTemplate(filename, pagesize=letter)
        elements = []
        styles = getSampleStyleSheet()
        # Logo at top right if available
        try:
            if os.path.exists(LOGO_FILENAME):
                logo_width = 1.1*inch
                logo_height = 1.1*inch
                img = RLImage(LOGO_FILENAME, width=logo_width, height=logo_height)
                title_para = Paragraph(f"<b>Case Summary Report</b>", styles["Title"])
                logo_table = Table(
                    [[title_para, img]],
                    colWidths=[None, logo_width],
                )
                logo_table.setStyle(TableStyle([
                    ("ALIGN", (0,0), (0,0), "LEFT"),
                    ("ALIGN", (1,0), (1,0), "RIGHT"),
                    ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                    ("LEFTPADDING", (1,0), (1,0), 6),
                    ("RIGHTPADDING", (1,0), (1,0), 0),
                    ("TOPPADDING", (0,0), (-1,-1), 2),
                    ("BOTTOMPADDING", (0,0), (-1,-1), 2),
                ]))
                elements.append(logo_table)
                elements.append(Spacer(1, 12))
            else:
                elements.append(Paragraph(f"<b>Case Summary Report</b>", styles["Title"]))
                elements.append(Spacer(1, 12))
        except Exception:
            elements.append(Paragraph(f"<b>Case Summary Report</b>", styles["Title"]))
            elements.append(Spacer(1, 12))
        # Build table of fields
        field_map = [(k, v['text']) for k, v in self.tree_columns_config.items() if k != 'id']
        data = []
        for key, label in field_map:
            val = case.get(key, "")
            if key in ('start_date', 'end_date', 'created_at'):
                val = format_date_str_for_display(val)
            elif key == 'fpr_complete':
                val = format_bool_int(val)
            elif key == 'data_recovered':
                # data_recovered is already stored as "Yes"/"No" strings, not 0/1 integers
                # Just use the value as-is, or show empty string if None/empty
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
        doc.build(elements)
        self._show_report_saved_dialog(filename)
    def show_custom_report_builder(self):
        """Show a dialog for building a custom report from selected columns and export as PDF/XLSX."""
        import tkinter as tk
        from tkinter import Toplevel, Checkbutton, IntVar, Button, Label, StringVar, Radiobutton
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
        # Filter: all, filtered, or selected rows
        filter_var = StringVar(value="all")
        Label(win, text="Rows to include:").grid(row=len(all_columns)+4, column=0, sticky='w', padx=10, pady=(10,2))
        Radiobutton(win, text="All cases", variable=filter_var, value="all").grid(row=len(all_columns)+5, column=0, sticky='w', padx=20)
        Radiobutton(win, text="Filtered (current search)", variable=filter_var, value="filtered").grid(row=len(all_columns)+6, column=0, sticky='w', padx=20)
        Radiobutton(win, text="Selected rows only", variable=filter_var, value="selected").grid(row=len(all_columns)+7, column=0, sticky='w', padx=20)
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
                self.export_custom_report_pdf(headers, rows)
            else:
                self.export_custom_report_xlsx(headers, rows)
            win.destroy()
        btn = Button(win, text="Export", command=do_export)
        btn.grid(row=len(all_columns)+8, column=0, pady=15)

    def export_custom_report_pdf(self, headers, rows):
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
            # Determine optimal page orientation based on content
            num_cols = len(headers)
            
            # Use landscape for more than 6 columns, portrait otherwise
            use_landscape = num_cols > 6
            pagesize = landscape(letter) if use_landscape else letter
            
            doc = SimpleDocTemplate(
                filename,
                pagesize=pagesize,
                rightMargin=10,
                leftMargin=10,
                topMargin=20,
                bottomMargin=20
            )
            
            styles = getSampleStyleSheet()
            
            # Create a custom style for table cells with better wrapping
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
            
            # Calculate column widths dynamically based on content
            page_width = pagesize[0] - (doc.leftMargin + doc.rightMargin)

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

            # Enforce min/max per column and guarantee total fits within page_width
            min_width = 0.35 * inch
            max_width = 2.4 * inch

            fixed_widths = {}
            remaining = set(range(len(col_weights)))
            remaining_width = page_width
            while True:
                total_weight = sum(col_weights[i] for i in remaining) or 1.0
                changed = False
                temp_widths = {}
                for i in remaining:
                    temp_widths[i] = (col_weights[i] / total_weight) * remaining_width
                to_fix = set()
                for i, w in temp_widths.items():
                    if w < min_width:
                        fixed_widths[i] = min_width
                        to_fix.add(i)
                        changed = True
                    elif w > max_width:
                        fixed_widths[i] = max_width
                        to_fix.add(i)
                        changed = True
                if not changed:
                    col_widths = [0.0] * len(col_weights)
                    for i in remaining:
                        col_widths[i] = temp_widths[i]
                    for i, w in fixed_widths.items():
                        col_widths[i] = w
                    break
                for i in to_fix:
                    remaining.discard(i)
                remaining_width = page_width - sum(fixed_widths.values())
                if remaining_width <= 0 or not remaining:
                    col_widths = [fixed_widths.get(i, min_width) for i in range(len(col_weights))]
                    break
            
            # Convert all table data to Paragraphs for proper word wrapping
            table_data = []
            
            # Process headers
            header_row = []
            for header in headers:
                header_row.append(Paragraph(str(header), cell_style))
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
                ("FONTSIZE", (0,0), (-1,0), 8),
                ("ALIGN", (0,0), (-1,0), "CENTER"),
                ("VALIGN", (0,0), (-1,0), "MIDDLE"),
                
                # Data row styling
                ("BACKGROUND", (0,1), (-1,-1), colors.white),
                ("TEXTCOLOR", (0,1), (-1,-1), colors.black),
                ("FONTNAME", (0,1), (-1,-1), "Helvetica"),
                ("FONTSIZE", (0,1), (-1,-1), 7),
                ("ALIGN", (0,1), (-1,-1), "LEFT"),
                ("VALIGN", (0,1), (-1,-1), "TOP"),
                
                # Grid and borders
                ("GRID", (0,0), (-1,-1), 0.5, colors.black),
                ("LINEBELOW", (0,0), (-1,0), 1, colors.red),
                
                # Padding for better spacing
                ("LEFTPADDING", (0,0), (-1,-1), 3),
                ("RIGHTPADDING", (0,0), (-1,-1), 3),
                ("TOPPADDING", (0,0), (-1,-1), 3),
                ("BOTTOMPADDING", (0,0), (-1,-1), 6),
                
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
            
            # Header info at top right
            header_info = self.get_report_header_info()
            now_str = datetime.now().strftime('%Y-%m-%d')
            header_lines = [
                f"Name: {header_info.get('Name','')}",
                f"Agency: {header_info.get('Agency','')}",
                f"Division: {header_info.get('Division','')}",
                f"Date: {now_str}"
            ]
            header_table = Table([[Paragraph(line, styles["Normal"])] for line in header_lines], hAlign='RIGHT')
            elements.append(header_table)
            elements.append(Spacer(1, 12))
            
            # Logo at top right if available
            try:
                if os.path.exists(LOGO_FILENAME):
                    logo_width = 1.1*inch
                    logo_height = 1.1*inch
                    img = RLImage(LOGO_FILENAME, width=logo_width, height=logo_height)
                    elements.append(img)
                    elements.append(Spacer(1, 12))
            except Exception:
                pass
            
            # Optional totals (Devices and Total Volume) below header before the table
            try:
                total_devices = len(rows)
                # Try to find a volume column in headers to sum
                vol_idx = None
                for i, h in enumerate(headers):
                    h_str = str(h).strip().lower()
                    if ('volume' in h_str and 'gb' in h_str) or h_str in ("vol (gb)", "volume_size_gb"):
                        vol_idx = i
                        break
                total_gb = None
                if vol_idx is not None:
                    total_gb = 0.0
                    for r in rows:
                        try:
                            total_gb += safe_float_conversion(r[vol_idx])
                        except Exception:
                            continue
                # Render totals
                elements.append(Paragraph(f"Total Devices: <b>{total_devices}</b>", styles['Normal']))
                if isinstance(total_gb, (int, float)):
                    total_tb = total_gb / 1024.0 if total_gb > 999 else None
                    vol_text = f"{total_tb:.2f} TB" if total_tb else f"{total_gb:.2f} GB"
                    elements.append(Paragraph(f"Total Volume: <b>{vol_text}</b>", styles['Normal']))
                elements.append(Spacer(1, 8))
            except Exception:
                pass

            elements.append(table)
            doc.build(elements)
            Messagebox.show_info("Report Exported", f"Custom PDF report saved to:\n{filename}")
            
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
            import pandas as pd
            pd.DataFrame([header_dict]).to_excel(writer, index=False, sheet_name='Header Info')
        Messagebox.show_info("Report Exported", f"Custom Excel report saved to:\n{filename}")
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
            self.map_widget.delete_all_markers()  # type: ignore[attr-defined]
        self._clear_heatmap()
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
        self._location_metrics = {}
        self._heatmap_points = []
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
        self._refresh_heatmap_if_needed()

    def _place_map_marker(self, city, state, coords):
        """Helper to place a marker on the map for a city/state with given coords."""
        try:
            lat, lon = coords
            city_cases = self._grouped_cases_by_location.get((city, state), [])
            offense_counts = {}
            total_volume = 0.0
            for case in city_cases:
                offense = (case.get('offense_type') or '').strip()
                if offense:
                    offense_counts[offense] = offense_counts.get(offense, 0) + 1
                total_volume += safe_float_conversion(case.get('volume_size_gb'))

            offenses_sorted = sorted(offense_counts.keys())
            offense_str = ', '.join(offenses_sorted) if offenses_sorted else 'No offenses recorded'
            marker_icon = getattr(self, 'marker_icon_tk_map', None)
            marker = self.map_widget.set_marker(  # type: ignore[attr-defined]
                lat, lon,
                text="",
                icon=marker_icon if marker_icon else DEFAULT_MARKER_ICON,
                command=lambda marker, loc=(city, state): self.on_marker_click(loc)
            )
            self.map_markers[(city, state)] = marker
            self._location_metrics[(city, state)] = {
                "total_cases": len(city_cases),
                "offense_counts": offense_counts,
                "total_volume_gb": total_volume,
            }
            repeats = max(1, len(city_cases))
            self._heatmap_points.extend([(lat, lon)] * repeats)
            logging.info(f"[MapMarkers] Marker set for {city}, {state} at ({lat}, {lon}) with {len(city_cases)} case(s)")
        except Exception as e:
            logging.error(f"[MapMarkers] Failed to set marker for {city}, {state}: {e}")

    # Clustering removed: no _rebuild_cluster_markers or cluster click handlers

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
                location = geolocator.geocode(f"{city}, {state}, USA")
                if location:
                    lat = getattr(location, "latitude", None)
                    lon = getattr(location, "longitude", None)
                    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                        coords = (float(lat), float(lon))
                        add_cached_location_db(f"{city}|{state}", coords[0], coords[1])
                        self._geocoded_results.append((city, state, coords))
                        logging.info(f"[MapMarkers] Geocoded {city}, {state}: {coords}")
                    else:
                        logging.warning(f"[MapMarkers] Geocode missing coords for {city}, {state}")
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
            self._refresh_heatmap_if_needed()
        # Continue polling if thread is alive and queue not empty
        if hasattr(self, 'geocoding_thread') and self.geocoding_thread and self.geocoding_thread.is_alive():
            self._geocoding_after_id = self.root.after(500, self._process_geocoding_results)
        else:
            self.processing_queue = False
            if self.map_status_label:
                self.map_status_label.config(text=f"Map status: {len(self.map_markers)} locations loaded (all done)")
            # Heatmap removed: no final overlay rendering
    # Removed duplicate import_cases_from_xlsx function - using the newer version below

    def _clear_heatmap(self):
        """Remove any existing heatmap overlay from the map widget."""
        try:
            if getattr(self, '_heatmap_layer', None):
                try:
                    if hasattr(self._heatmap_layer, 'delete'):
                        self._heatmap_layer.delete()  # type: ignore[attr-defined]
                    elif hasattr(self.map_widget, 'delete'):
                        self.map_widget.delete(self._heatmap_layer)  # type: ignore[attr-defined]
                except Exception:
                    pass
        finally:
            self._heatmap_layer = None

    def _render_heatmap(self):
        """Render the heatmap overlay when enabled and supported."""
        if not getattr(self, '_heatmap_enabled', False):
            return
        if not self.map_widget or not hasattr(self.map_widget, 'set_heatmap'):
            return
        if not self._heatmap_points:
            self._clear_heatmap()
            return
        try:
            self._clear_heatmap()
            self._heatmap_layer = self.map_widget.set_heatmap(self._heatmap_points)  # type: ignore[attr-defined]
        except Exception as e:
            logging.warning(f"Heatmap rendering failed: {e}")
            self._heatmap_layer = None

    def _refresh_heatmap_if_needed(self):
        """Update or clear the heatmap overlay based on the current toggle state."""
        if getattr(self, '_heatmap_enabled', False):
            self._render_heatmap()
        else:
            self._clear_heatmap()

    def toggle_heatmap(self):
        """Callback from the UI toggle to enable or disable the heatmap."""
        enabled = bool(self.heatmap_var.get()) if hasattr(self, 'heatmap_var') else False
        if not hasattr(self.map_widget, 'set_heatmap'):
            enabled = False
        self._heatmap_enabled = enabled
        set_user_pref('map_heatmap_enabled', '1' if enabled else '0')
        self._refresh_heatmap_if_needed()

    def on_closing(self):
        """Safely handle application shutdown: stop timers/threads, backup DB, release images, and destroy the UI."""
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

        # Attempt a final backup on close (keep last 5)
        try:
            perform_db_backup(retention_days=56, keep_last=5)
        except Exception as e:
            logging.warning(f"Close backup failed: {e}")

        # Always destroy the map widget if it exists, to prevent background update errors
        try:
            if hasattr(self, 'map_widget') and self.map_widget is not None:
                try:
                    if hasattr(self.map_widget, 'destroy'):
                        self.map_widget.destroy()
                except Exception as e:
                    logging.warning(f"Error destroying map widget: {e}")
                finally:
                    self.map_widget = None
        except Exception as e:
            logging.warning(f"Error during map widget cleanup: {e}")

        # Stop any background processing loops
        self.processing_queue = False

        # Release image references to help cleanup on Windows
        try:
            if hasattr(self, 'logo_image_tk'):
                self.logo_image_tk = None
            if hasattr(self, 'logo_image_tk_preview'):
                self.logo_image_tk_preview = None
            if hasattr(self, 'marker_icon_tk_map'):
                self.marker_icon_tk_map = None
            if hasattr(self, 'marker_icon_tk_preview'):
                self.marker_icon_tk_preview = None
            # Clear global default marker image
            global DEFAULT_MARKER_ICON
            DEFAULT_MARKER_ICON = None
        except Exception:
            pass

        # Destroy the main window
        try:
            if hasattr(self, 'root'):
                try:
                    self.root.quit()
                except Exception:
                    pass
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

        # --- Dashboard summary StringVars (must be initialized before any method uses them) ---
        import tkinter as tk
        self.total_cases_var = tk.StringVar(value="0")
        self.critical_var = tk.StringVar(value="Critical: 0")
        self.high_var = tk.StringVar(value="High: 0")
        self.medium_var = tk.StringVar(value="Medium: 0")
        self.low_var = tk.StringVar(value="Low: 0")
        self.overdue_var = tk.StringVar(value="Overdue: 0")
        self.due_soon_var = tk.StringVar(value="Due Soon: 0")
        self.total_volume_var = tk.StringVar(value="0 GB")

        # When theme changes at runtime, re-apply contrast-aware colors on summary labels
        try:
            self.root.bind('<<ThemeChanged>>', lambda e: self.root.after_idle(self.refresh_contrast_colors))
        except Exception:
            pass

        # Attributes for entry widgets
        self.entries = {}  # Dictionary to hold Tkinter variables/widgets for form fields
        self.editing_case_id = None  # Variable to track if we are currently editing a case (None or case_id)
        self.submit_button = None  # Reference to the submit button for text changes
        self.in_progress_button = None  # Reference to the in progress button
        self.field_frame_container = None  # Reference to the frame holding input fields

        # Attributes for in-progress cases
        self.in_progress_tree = None  # Treeview for in-progress cases
        self.editing_in_progress_case_id = None  # Variable to track if we are currently editing an in-progress case

        # Attributes for logo image
        self.logo_path = tk.StringVar(value=LOGO_FILENAME)  # Track the path, though we primarily use the loaded image
        self.logo_image_tk = None  # Image for display in the Entry tab (scaled)
        self.logo_image_tk_preview = None  # Separate image for the settings preview (thumbnail)
        self.entry_logo_label = None  # Attribute to store the logo label in the Entry tab (needed to update its image)
        self.logo_preview_canvas = None  # Reference to the settings logo preview canvas

        # Attributes for marker icon images
        self.marker_icon_tk_map = None  # Image for map markers (20x20)
        self.marker_icon_tk_preview = None  # Image for settings preview (e.g., 50x50)
        self.marker_icon_preview_canvas = None  # Reference to the settings preview canvas

        # Initialize editable combobox registry early
        try:
            self._init_combo_registry()
        except Exception:
            pass

    # Map extras removed (no heatmap or clustering)

        self.load_logo_image()  # Load the logo upon app initialization
        self.load_marker_icon_image()  # Load the marker icon upon app initialization

        # Attributes for Map View
        self.map_widget = None
        self.map_status_label = None
        # Geopy geolocator instance - only create one per thread. Not needed in main thread.
        # self.geolocator = Nominatim(user_agent=APP_NAME)
        self.map_markers = {}  # Dictionary to hold mapview markers with location (city, state) as key
        self._grouped_cases_by_location = {}  # Store cases grouped by location for info bubbles
        self._location_metrics = {}  # Cache aggregated metrics (counts, volume) per location
        self._heatmap_layer = None
        self._heatmap_points = []
        saved_heatmap_pref = get_user_pref('map_heatmap_enabled', '0')
        self._heatmap_enabled = str(saved_heatmap_pref).lower() in ('1', 'true', 'yes', 'on', 'y')

        # Attributes for View Data Treeview
        self.tree = None
        self.tree_columns_config = {}  # Dictionary to store treeview column configuration
        self.treeview_sort_column = None  # To keep track of the currently sorted column
        self.treeview_sort_reverse = False  # To keep track of the sort order

        # Attributes for Graph Tab
        self.fig = None  # Matplotlib figure
        self.ax = None  # Matplotlib axes
        self.canvas_agg = None  # FigureCanvasTkAgg

        # Attributes for Status Bar
        self.status_label = None
        self.status_animation_id = None
        self.status_text = ""

        # Attributes for threading and queue for map loading
        self.geocoding_queue = queue.Queue()
        self.geocoding_thread = None
        self.processing_queue = False  # Flag to indicate if we are currently checking the queue
        self.geolocated_count = 0  # Initialize count for geolocated markers (locations)
        self.skipped_count = 0  # Initialize count for skipped locations
        self._geocoding_after_id = None  # ID for the scheduled _process_geocoding_results after call

        # Always ensure DB is initialized before any data access
        try:
            init_db()
        except Exception as e:
            logging.error(f"Failed to initialize database at startup: {e}")
            Messagebox.show_error("Database Error", f"Failed to initialize database: {e}")
            # Optionally, exit or disable UI

        self.create_widgets()  # Create all the main UI widgets

        # Status Bar creation (Moved here to ensure self.status_label exists before status updates)
        self.status_label = ttk.Label(self.root, text="Initializing...", anchor='w')
        self.status_label.grid(row=1, column=0, sticky='ew', padx=10, pady=(0, 5))
        self.update_status("Initializing...")

        # Perform initial data loading and UI refresh
        self.refresh_data_view()  # Populate the treeview
        # Only refresh in-progress view if the widget exists (created after create_in_progress_widgets)
        if hasattr(self, 'in_progress_tree') and self.in_progress_tree:
            self.refresh_in_progress_view()  # Populate the in-progress treeview
        self.load_map_markers()  # This now starts the threaded geocoding
        self.populate_graph_filters()  # Populate filters for the graph
        self.update_graph()  # Display initial graph

        # Initial status is set by the map loading process or defaults below if map loading is skipped
        # The _finalize_map_loading will set the final status
        # Ensure status is cleared if thread finishes quickly
        if not self.geocoding_thread or not self.geocoding_thread.is_alive():
            self.update_status("Ready")

        # Set the window closing protocol to call the cleanup function
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
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
        self.in_progress_frame = tb.Frame(self.notebook, padding="10")
        # Activity Timeline removed for a sleeker UI
        self.map_frame = tb.Frame(self.notebook, padding="10")
        self.graph_frame = tb.Frame(self.notebook, padding="10")
        self.settings_frame = tb.Frame(self.notebook, padding="10")
        self.about_frame = tb.Frame(self.notebook, padding="10")

        for frame in [self.entry_frame, self.view_frame, self.in_progress_frame, self.map_frame, self.graph_frame, self.settings_frame, self.about_frame]:
            frame.rowconfigure(0, weight=1)
            frame.columnconfigure(0, weight=1)

        self.notebook.add(self.entry_frame, text='New Case Entry')
        self.notebook.add(self.view_frame, text='View Data')
        self.notebook.add(self.in_progress_frame, text='In Progress')
        self.notebook.add(self.map_frame, text='Map View')
        self.notebook.add(self.graph_frame, text='Graphs')
        self.notebook.add(self.settings_frame, text='Settings')
        self.notebook.add(self.about_frame, text='About')

        self.create_entry_widgets()
        self.create_view_widgets()
        self.create_in_progress_widgets()
        self.create_map_widgets()
        self.create_graph_widgets()
        self.create_settings_widgets()
        self.create_about_widgets()

        # Auto-update check on launch if enabled
        if get_user_pref('auto_update_check', True):
            self.check_for_updates(silent=True)
        # Ensure no call to create_dashboard_widgets remains

    def create_about_widgets(self):
        """Creates the widgets for the About tab with application info."""
        about_text = (
            f"{APP_NAME}\n\n"
            "A digital forensics case log and reporting tool for labs and agencies.\n\n"
            "Program Functions & Features:\n"
            "- Case Entry: Add cases with examiner, agency, offense, device, model, OS, notes, dates, and more.\n"
            "- In Progress Tracking: Priority, workflow status, target due date; quick 'Mark as Completed'.\n"
            "- View Data: Fast search/filter, inline editing with validation, undo/redo, and context menu (open/edit/duplicate/export).\n"
            "- Saved Views: Persisted filters and column presets for faster workflows.\n"
            "- Reports: Standardized portrait PDFs (All, Summary, Custom, Date Range) with repeated headers/footers and dynamic sizing; export to XLSX.\n"
            "- Report Header: Persist Name/Agency/Division/Date across sessions.\n"
            "- Map View: Markers grouped by City, ST with on-click offense counts; Fit To Locations; hover tooltips; optional focal state; cached geocoding.\n"
            "- Graphs: Offense, device, agency, examiner, and more with optional year filter.\n"
            "- Backups: Automatic on close + manual; keep last 5; quick restore and 'Open Backups Folder'.\n"
            "- DB Health & Performance: Indexed tables; one-click VACUUM/ANALYZE; geocache hit/miss counters.\n"
            "- Imports: Cancellable Excel import worker with post-import optimization.\n"
            "- Settings: Themes, header logo, custom map marker icon; scrollable settings panel.\n"
            "- Security: Password-protected sensitive actions.\n\n"
            "What's New in v2.1.1 (Aug 27, 2025):\n"
            "- Map: Added 'Fit To Locations' and marker hover tooltips (City, ST — N cases).\n"
            "- Reports: Polished portrait layouts with repeated headers and better page fit.\n"
            "- Settings: Persisted logo and custom marker icon with instant preview; scrollable panel.\n"
            "- Backups: Keep last 5 backups with restore and open-folder shortcuts.\n"
            "- Reliability: Safer numeric parsing and improved Excel import cancellation.\n"
            "- Simplification: Removed Activity Timeline module.\n"
            "- Packaging: Cleaned requirements and ensured assets bundle into the EXE.\n\n"
            "- Accessibility: Dashboard totals auto-switch black/white on theme change instantly.\n"
            "- Productivity: Editable comboboxes now support Add/Delete/Manage with live updates.\n\n"
            "How to Use (Quick Guide):\n"
            "- New Case Entry: Fill required fields and Submit. Use 'In Progress' to track active cases.\n"
            "- In Progress: Edit, set priority/due date, or 'Mark as Completed' to move to View Data.\n"
            "- View Data: Search, filter, edit inline (Enter to edit, Del to delete, right-click for menu). Export selected rows.\n"
            "- Reports: Use toolbar buttons for All Cases, Summary, Custom, and Date Range (portrait by default).\n"
            "- Map View: Optionally pick a focal state; click markers for offense counts; try 'Fit To Locations'.\n"
            "- Graphs: Select a graph type and (optionally) filter by year.\n"
            "- Settings: Change theme, header logo, and marker icon; changes preview instantly.\n"
            "- Tips: Total volume auto-switches GB↔TB. Many fields remember your last values.\n\n"
            "XLSX Import Format:\n"
            "When importing from Excel, include these columns (exact, case-sensitive):\n"
            "- ID (optional) | Case # | Examiner | Investigator | Agency | City | State |\n"
            "  Start (MM-DD-YYYY) | End (MM-DD-YYYY) | Vol (GB) | Offense | Device | Model | OS |\n"
            "  Recovered? | FPR? | Notes | Created (YYYY-MM-DD)\n\n"
            "Data Storage:\n"
            "- All case data is stored locally in a SQLite database (caselog_gui_v6.db).\n"
            "- User preferences and settings are stored in the app_data directory.\n\n"
            "Support & Documentation:\n"
            "- For help or updates, contact your system administrator or the application provider.\n"
            "- This tool is for internal use by digital forensics labs and law enforcement.\n\n"
            f"Version: {APP_VERSION} ({RELEASE_DATE})\n"
            f"Data Directory: {DATA_DIR}\n"
            f"Database File: {DB_FILENAME}\n"
            f"Log File: {LOG_FILENAME}\n\n"
            "Developer: RF-YVY\n"
            "GitHub: https://github.com/RF-YVY\n"
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

        # Add Check for Updates button on About tab for convenience
        try:
            updates_frame = ttk.Frame(self.about_frame)
            updates_frame.grid(row=2, column=0, sticky='w', padx=14, pady=(0, 12))
            ttk.Button(
                updates_frame,
                text=f"Check for Updates (v{APP_VERSION})",
                command=lambda: self.check_for_updates(silent=False)
            ).pack(side='left')
        except Exception:
            pass

    # Dashboard tab and window have been removed as requested.

    def create_entry_widgets(self):
        """Creates the widgets for the New Case Entry tab."""
        # Layout: fixed header (row 0) + scrollable content (row 1)
        self.entry_frame.rowconfigure(0, weight=0)
        self.entry_frame.rowconfigure(1, weight=1)
        self.entry_frame.columnconfigure(0, weight=1)

        # Header with title (left) and logo (fixed at top-right)
        header_frame = tb.Frame(self.entry_frame)
        header_frame.grid(row=0, column=0, sticky='ew')
        header_frame.columnconfigure(0, weight=1)
        header_frame.columnconfigure(1, weight=0)

        title_label = tb.Label(header_frame, text="New Case Entry", font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, sticky='w', padx=10, pady=(8, 4))

        # Fixed logo at far top-right (image set in load_logo_image)
        self.entry_logo_label = ttk.Label(header_frame, text="No Logo")
        self.entry_logo_label.grid(row=0, column=1, sticky='e', padx=10, pady=(8, 4))
        try:
            self.load_logo_image()
        except Exception:
            pass

        # Create a main frame that will hold the scrollable content
        entry_content_frame = tb.Frame(self.entry_frame)
        entry_content_frame.grid(row=1, column=0, sticky='nsew')

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
            ("Forensic Tool", "forensic_tool", "combo", ["Cellebrite", "GrayKey"]),
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
                if key in ["examiner", "investigator", "agency", "offense_type", "city_of_offense", "forensic_tool"]:
                    # Merge persisted + derived values for first render
                    combo_values = self._get_initial_combo_values(key)
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

                # --- Add dynamic entry + context menu for editable combos ---
                if key in ["examiner", "investigator", "agency", "offense_type", "city_of_offense", "forensic_tool"]:
                    # Ensure registry exists
                    if not hasattr(self, '_combo_registry'):
                        self._init_combo_registry()
                    # Add to registry and attach menu
                    self._register_editable_combo(key, combo, var)
                    # Add on-the-fly persistence and live refresh
                    def add_to_combo(event, var=var, key=key):
                        value = var.get().strip()
                        if value:
                            self._add_value_to_combo_list(key, value)
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

        # --- Phase 1: Additional In-Progress Fields ---
        # Add priority, due date, and progress percentage fields
        phase1_row = check_row + 1
        phase1_frame = ttk.Frame(self.field_frame_container, padding=(0,0,10,5))
        phase1_frame.grid(row=phase1_row, column=0, columnspan=2, sticky='ew', padx=5, pady=2)
        self.field_frame_container.grid_rowconfigure(phase1_row, weight=0)

        # Priority field (left side)
        priority_frame = ttk.Frame(phase1_frame)
        priority_frame.pack(side='left', fill='x', expand=True, padx=(0, 10))
        priority_label = ttk.Label(priority_frame, text="Priority")
        priority_label.pack(side='top', anchor='w')
        priority_var = tk.StringVar(value='Medium')
        priority_combo = ttk.Combobox(priority_frame, textvariable=priority_var, 
                                    values=['Low', 'Medium', 'High', 'Critical'], state='readonly', width=20)
        priority_combo.pack(side='top', fill='x')
        self.entries['priority'] = priority_var

        # Phase 3: Workflow Status field (middle)
        workflow_frame = ttk.Frame(phase1_frame)
        workflow_frame.pack(side='left', fill='x', expand=True, padx=(0, 10))
        workflow_label = ttk.Label(workflow_frame, text="Workflow Status")
        workflow_label.pack(side='top', anchor='w')
        workflow_var = tk.StringVar(value='Intake')
        workflow_combo = ttk.Combobox(workflow_frame, textvariable=workflow_var, 
                                    values=['Intake', 'Processing', 'Reporting', 'In Vault', 'Ready for Completion'], 
                                    state='readonly', width=20)
        workflow_combo.pack(side='top', fill='x')
        self.entries['workflow_status'] = workflow_var

        # Due Date field (right side) - Use DateEntry for calendar picker
        due_date_frame = ttk.Frame(phase1_frame)
        due_date_frame.pack(side='left', fill='x', expand=True)
        due_date_label = ttk.Label(due_date_frame, text="Target Due Date (Click calendar icon)")
        due_date_label.pack(side='top', anchor='w')
        
        # Use DateEntry widget for calendar functionality (already imported as tb.DateEntry)
        due_date_widget = DateEntry(due_date_frame, width=18, dateformat='%m-%d-%Y')
        due_date_widget.pack(side='top', fill='x')
        self.entries['target_due_date'] = due_date_widget

        # --- Notes field ---
        # Place notes field after the Phase 1 fields
        notes_row = phase1_row + 1  # Place notes below Phase 1 fields
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

        # Add In Progress button
        self.in_progress_button = tb.Button(submit_button_frame, text="In Progress", command=self.submit_in_progress_case, style="Warning.TButton")
        self.in_progress_button.pack(side='left', padx=(5,0)) # Pack next to submit button

        # Add a Cancel Edit/Clear Form button
        cancel_button = ttk.Button(submit_button_frame, text="Clear Form", command=self.clear_entry_form)
        cancel_button.pack(side='left', padx=(5,0)) # Pack next to in progress button

        # Configure Accent button style (defined here as used in this tab)
        self.style.configure("Accent.TButton", font=("-weight", "bold"))
        
        # Configure Danger button style (for delete buttons)
        self.style.configure("Danger.TButton", foreground="red", font=("-weight", "bold"))


        # After all widgets are created in create_entry_widgets: live-repopulate editable combos
        if not hasattr(self, '_combo_registry'):
            self._init_combo_registry()
        for key in ["examiner", "investigator", "agency", "offense_type", "city_of_offense", "forensic_tool"]:
            try:
                self._refresh_registered_combos(key, get_combo_values_db(key))
            except Exception:
                pass

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
        # Save reference for Ctrl+F focus
        self.view_search_entry = search_entry
        search_entry.bind('<Return>', lambda e: self.apply_view_filter())

        # Saved Filters menu
        self.filters_button = ttk.Menubutton(search_frame, text="Filters", direction='below')
        self.filters_button.pack(side='left', padx=(10, 0))
        self.filters_menu = tk.Menu(self.filters_button, tearoff=0)
        self.filters_button.configure(menu=self.filters_menu)
        self.rebuild_filters_menu()

        # Reports button with dropdown menu
        reports_button = ttk.Menubutton(search_frame, text="Reports", direction='below')
        reports_button.pack(side='left', padx=(10, 0))
        reports_menu = tk.Menu(reports_button, tearoff=0)
        reports_menu.add_command(label="Date Range", command=self.show_date_range_report)
        reports_menu.add_command(label="Case Summary", command=self.show_case_summary_report)
        reports_menu.add_command(label="Custom Report", command=self.show_custom_report_builder)
        reports_menu.add_command(label="All Cases Summary", command=self.show_all_cases_summary)
        reports_menu.add_separator()
        reports_menu.add_command(label="Export All Cases PDF", command=self.export_pdf_report)
        reports_menu.add_command(label="Export All Cases XLSX", command=self.export_xlsx_report)
        reports_button.configure(menu=reports_menu)

        # Add Columns button
        columns_button = ttk.Button(search_frame, text="Columns", command=self.show_column_selector)
        columns_button.pack(side='left', padx=(10, 0))

        # Column Presets menu (Save/Apply/Manage)
        self.col_presets_button = ttk.Menubutton(search_frame, text="Column Presets", direction='below')
        self.col_presets_button.pack(side='left', padx=(6, 0))
        self.col_presets_menu = tk.Menu(self.col_presets_button, tearoff=0)
        self.col_presets_button.configure(menu=self.col_presets_menu)
        self.rebuild_column_presets_menu()

        # Button frame for Refresh, Export, Edit, Delete, Undo, Redo
        button_frame = ttk.Frame(container)
        button_frame.grid(row=1, column=0, sticky='ew', pady=(0, 10), padx=5)
        refresh_button = ttk.Button(button_frame, text="Refresh Data", command=self.refresh_data_view)
        refresh_button.pack(side='left', padx=(0, 5))
        # Add Edit Selected button
        edit_button = ttk.Button(button_frame, text="Edit Selected", command=self.edit_selected_case)
        edit_button.pack(side='left', padx=(0,5))
        # Bulk Edit for multiple selected rows
        bulk_edit_btn = ttk.Button(button_frame, text="Bulk Edit…", command=self.bulk_edit_selected_rows)
        bulk_edit_btn.pack(side='left', padx=(0,5))
        # Add a Delete Selected button
        delete_button = ttk.Button(button_frame, text="Delete Selected", command=self.delete_selected_cases, style="Danger.TButton")
        delete_button.pack(side='left')
        # Add Undo/Redo buttons
        undo_button = ttk.Button(button_frame, text="Undo Edit", command=self.undo_view_edit)
        undo_button.pack(side='left', padx=(10,2))
        redo_button = ttk.Button(button_frame, text="Redo Edit", command=self.redo_view_edit)
        redo_button.pack(side='left', padx=(2,0))

        # Frame to hold the Treeview and its scrollbars
        tree_frame = ttk.Frame(container)
        tree_frame.grid(row=2, column=0, sticky='nsew', padx=5, pady=5)
        container.rowconfigure(2, weight=1)
        container.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(tree_frame, show='headings')

        # Store the database column names along with display text and other config
        # Ensure 'id' is included but marked as not visible
        self.tree_columns_config = {
            "id": {"text": "ID", "width": 0, "visible": False}, # Keep ID for deletion/editing but hide
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
            "forensic_tool": {"text": "Forensic Tool", "width": 120},
            "model": {"text": "Model", "width": 100},
            "os": {"text": "OS", "width": 80},
            "data_recovered": {"text": "Recovered?", "width": 70}, # Keep text, will display Yes/No
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
            # Configure headings only for displayed columns
            if col_key in visible_columns:
                self.tree.heading(col_key, text=config["text"], command=lambda c=col_key: self.sort_treeview_column(c))
            self.tree.column(col_key, anchor='w', width=config["width"], stretch=tk.NO)
            if not config.get("visible", True):
                self.tree.column(col_key, width=0, stretch=tk.NO) # Hide the column

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
        self.tree.bind('<Return>', lambda e: self.edit_selected_case())  # Enter to edit
        self.tree.bind('<Delete>', lambda e: self.delete_selected_cases())  # Delete key
        self.tree.bind('<Control-c>', lambda e: self.copy_selected_treeview_rows())  # Ctrl+C to copy
        self.tree.bind('<Control-e>', lambda e: self.edit_selected_case())  # Ctrl+E to edit
        self.tree.bind('<Control-f>', lambda e: self.focus_find())  # Ctrl+F to focus search
        self.tree.bind('<F5>', lambda e: self.refresh_data_view(reset_lazy=True))  # F5 refresh
        self.tree.bind('<Double-1>', self.on_treeview_cell_double_click)  # Inline edit cell
        self.tree.bind('<Tab>', lambda e: self.focus_next_widget(e))
        self.tree.bind('<Shift-Tab>', lambda e: self.focus_prev_widget(e))
        self.tree.bind('<FocusIn>', lambda e: self.update_status("Treeview focused. Use arrows, Enter to edit, Del to delete, right-click for menu."))
        self.tree.bind('<Button-3>', self.on_treeview_right_click)  # Windows context menu
        self.tree.bind('<Menu>', self.on_treeview_right_click)  # Keyboard context menu key
        self.tree['takefocus'] = True
        # Set accessible headings
        for col_key, config in self.tree_columns_config.items():
            self.tree.heading(col_key, text=config["text"], command=lambda c=col_key: self.sort_treeview_column(c))
        # Set accessible names for buttons
        for btn in [refresh_button, edit_button, delete_button, undo_button, redo_button]:
            btn['takefocus'] = True
            btn['cursor'] = 'hand2'
        # Add accessible labels to search/filter entry
        search_entry['takefocus'] = True
        search_entry['cursor'] = 'xterm'
        # 'aria-label' is not a valid Tkinter option; skip for compatibility
        # Add accessible tooltips (if available)
        try:
            from ttkbootstrap.tooltip import ToolTip
            ToolTip(search_entry, 'Type to search/filter cases. Press Enter to apply.')
        except Exception:
            pass

        # Always populate the table on tab creation
        self.refresh_data_view()


    def create_about_widgets(self):
        """Creates the widgets for the About tab with application info."""
        about_text = (
            f"CyberLab Case Tracker\n"
            f"\nPurpose:\n"
            "CyberLab Case Tracker is designed for digital forensic professionals to efficiently manage, track, and report on digital evidence and casework.\n\n"
            "Key Features:\n"
            "- Add, edit, and manage digital forensic cases with detailed metadata.\n"
            "- Track both in-progress and completed cases.\n"
            "- Generate comprehensive reports (PDF, text, Excel) for cases and date ranges.\n"
            "- Visualize case data with interactive graphs and maps.\n"
            "- Import/export case data (Excel, CSV).\n"
            "- Customizable columns, user preferences, and password protection.\n"
            "- Auto-update check and easy settings management.\n\n"
            "Quick Start Guide:\n"
            "1. Add a new case using the 'New Entry' tab.\n"
            "2. View, search, and filter all cases in the 'View Data' tab.\n"
            "3. Track ongoing work in the 'In Progress' tab.\n"
            "4. Visualize trends in the 'Graphs' tab and locations in the 'Map' tab.\n"
            "5. Generate and export reports from the 'Reports' menu.\n"
            "6. Adjust preferences and check for updates in the 'Settings' tab.\n\n"
            f"Version: v2.1.5 ({RELEASE_DATE})\n"
            "Developed by RF-YVY. For more info, visit: https://github.com/RF-YVY/CyberLabLog\n"
        )
        about_box = scrolledtext.ScrolledText(self.about_frame, wrap='word', font=("Segoe UI", 11), state='normal', height=28, width=100)
        about_box.insert('1.0', about_text)
        about_box.config(state='disabled')
        about_box.pack(fill='both', expand=True, padx=10, pady=(0,10))
        # Add Check for Updates button
        update_btn = ttk.Button(self.about_frame, text="Check for Updates", command=self.check_for_updates)
        update_btn.pack(anchor='e', padx=10, pady=(0,10))
    # --- Saved Filters and Column Presets Helpers ---
    def rebuild_filters_menu(self):
        """Rebuild the Filters menu from saved JSON settings."""
        try:
            self.filters_menu.delete(0, 'end')
        except Exception:
            pass
        # Load saved filters
        saved = get_json_setting('saved_filters', []) or []
        # Apply current filter entry
        self.filters_menu.add_command(label="Apply current", command=self.apply_view_filter)
        # Save current as…
        self.filters_menu.add_command(label="Save current as…", command=self.save_current_filter)
        self.filters_menu.add_separator()
        if not saved:
            self.filters_menu.add_command(label="(no saved filters)", state='disabled')
        else:
            for item in saved:
                name = item.get('name') or '(unnamed)'
                value = item.get('value') or ''
                self.filters_menu.add_command(label=f"Apply: {name}", command=lambda v=value: self.apply_named_filter(v))
            self.filters_menu.add_separator()
            self.filters_menu.add_command(label="Manage…", command=self.manage_saved_filters)

    def apply_named_filter(self, value: str):
        self.view_search_var.set(value or '')
        self.apply_view_filter()

    def save_current_filter(self):
        name = simpledialog.askstring("Save Filter", "Name for this filter:", parent=self.root)
        if not name:
            return
        value = self.view_search_var.get().strip()
        saved = get_json_setting('saved_filters', []) or []
        # Deduplicate by name
        saved = [s for s in saved if s.get('name') != name]
        saved.append({'name': name, 'value': value})
        set_json_setting('saved_filters', saved)
        self.rebuild_filters_menu()

    def manage_saved_filters(self):
        # Simple manager to rename/delete saved filters
        items = get_json_setting('saved_filters', []) or []
        win = tk.Toplevel(self.root)
        win.title("Manage Saved Filters")
        win.geometry("360x300")
        win.grab_set()
        frame = ttk.Frame(win)
        frame.pack(fill='both', expand=True, padx=10, pady=10)
        lb = tk.Listbox(frame)
        for s in items:
            lb.insert('end', s.get('name') or '(unnamed)')
        lb.pack(fill='both', expand=True)
        btns = ttk.Frame(frame)
        btns.pack(fill='x', pady=(8,0))
        def do_delete():
            sel = lb.curselection()
            if not sel:
                return
            idx = sel[0]
            del items[idx]
            set_json_setting('saved_filters', items)
            lb.delete(idx)
            self.rebuild_filters_menu()
        def do_rename():
            sel = lb.curselection()
            if not sel:
                return
            idx = sel[0]
            new_name = simpledialog.askstring("Rename Filter", "New name:", parent=win)
            if not new_name:
                return
            items[idx]['name'] = new_name
            set_json_setting('saved_filters', items)
            lb.delete(idx)
            lb.insert(idx, new_name)
            self.rebuild_filters_menu()
        ttk.Button(btns, text="Rename", command=do_rename).pack(side='left')
        ttk.Button(btns, text="Delete", command=do_delete).pack(side='left', padx=(6,0))
        ttk.Button(btns, text="Close", command=win.destroy).pack(side='right')

    def rebuild_column_presets_menu(self):
        try:
            self.col_presets_menu.delete(0, 'end')
        except Exception:
            pass
        presets = get_json_setting('column_presets', []) or []
        self.col_presets_menu.add_command(label="Save current as…", command=self.save_current_columns_preset)
        self.col_presets_menu.add_separator()
        if not presets:
            self.col_presets_menu.add_command(label="(no presets)", state='disabled')
        else:
            for p in presets:
                name = p.get('name') or '(unnamed)'
                cols = p.get('columns') or []
                self.col_presets_menu.add_command(label=f"Apply: {name}", command=lambda c=cols: self.set_visible_treeview_columns(c))
            self.col_presets_menu.add_separator()
            self.col_presets_menu.add_command(label="Manage…", command=self.manage_column_presets)

    def save_current_columns_preset(self):
        name = simpledialog.askstring("Save Columns Preset", "Name for this preset:", parent=self.root)
        if not name:
            return
        cols = list(self.get_visible_treeview_columns())
        presets = get_json_setting('column_presets', []) or []
        presets = [p for p in presets if p.get('name') != name]
        presets.append({'name': name, 'columns': cols})
        set_json_setting('column_presets', presets)
        self.rebuild_column_presets_menu()

    def manage_column_presets(self):
        presets = get_json_setting('column_presets', []) or []
        win = tk.Toplevel(self.root)
        win.title("Manage Column Presets")
        win.geometry("360x300")
        win.grab_set()
        frame = ttk.Frame(win)
        frame.pack(fill='both', expand=True, padx=10, pady=10)
        lb = tk.Listbox(frame)
        for p in presets:
            lb.insert('end', p.get('name') or '(unnamed)')
        lb.pack(fill='both', expand=True)
        btns = ttk.Frame(frame)
        btns.pack(fill='x', pady=(8,0))
        def do_delete():
            sel = lb.curselection()
            if not sel:
                return
            idx = sel[0]
            del presets[idx]
            set_json_setting('column_presets', presets)
            lb.delete(idx)
            self.rebuild_column_presets_menu()
        def do_rename():
            sel = lb.curselection()
            if not sel:
                return
            idx = sel[0]
            new_name = simpledialog.askstring("Rename Preset", "New name:", parent=win)
            if not new_name:
                return
            presets[idx]['name'] = new_name
            set_json_setting('column_presets', presets)
            lb.delete(idx)
            lb.insert(idx, new_name)
            self.rebuild_column_presets_menu()
        ttk.Button(btns, text="Rename", command=do_rename).pack(side='left')
        ttk.Button(btns, text="Delete", command=do_delete).pack(side='left', padx=(6,0))
        ttk.Button(btns, text="Close", command=win.destroy).pack(side='right')

    def bulk_edit_selected_rows(self):
        # Edit one field across selected completed cases
        sel = self.tree.selection()
        if not sel:
            Messagebox.show_info("Bulk Edit", "Select one or more rows first.")
            return
        # Build a map of editable fields (exclude 'id')
        field_map = [(k, v['text']) for k, v in self.tree_columns_config.items() if k != 'id']
        win = tk.Toplevel(self.root)
        win.title("Bulk Edit Selected")
        win.grab_set()
        frm = ttk.Frame(win, padding=10)
        frm.pack(fill='both', expand=True)
        ttk.Label(frm, text="Field:").grid(row=0, column=0, sticky='w')
        field_var = tk.StringVar(value=field_map[0][0])
        field_combo = ttk.Combobox(frm, values=[f"{k} — {label}" for k,label in field_map], state='readonly', width=40)
        field_combo.current(0)
        field_combo.grid(row=0, column=1, sticky='ew')
        ttk.Label(frm, text="New value:").grid(row=1, column=0, sticky='w', pady=(8,0))
        value_var = tk.StringVar()
        value_entry = ttk.Entry(frm, textvariable=value_var, width=40)
        value_entry.grid(row=1, column=1, sticky='ew', pady=(8,0))
        frm.columnconfigure(1, weight=1)
        def parse_field_key():
            sel_txt = field_combo.get()
            return sel_txt.split(' — ')[0] if ' — ' in sel_txt else sel_txt
        def do_apply():
            field_key = parse_field_key()
            new_val = value_var.get()
            ids = [self.tree.item(i)['values'][0] for i in sel]
            updated = 0
            for case_id in ids:
                case = get_case_by_id_db(case_id)
                if not case:
                    continue
                case[field_key] = new_val
                if update_case_db(case_id, case):
                    updated += 1
            self.refresh_data_view(reset_lazy=False)
            Messagebox.show_info("Bulk Edit", f"Updated {updated} row(s).")
            win.destroy()
        btns = ttk.Frame(frm)
        btns.grid(row=2, column=0, columnspan=2, pady=(12,0), sticky='e')
        ttk.Button(btns, text="Apply", command=do_apply).pack(side='right')
        ttk.Button(btns, text="Cancel", command=win.destroy).pack(side='right', padx=(0,6))

    # --- Context Menu for Treeview ---
    def on_treeview_right_click(self, event):
        # Show context menu on right-click or Menu key
        iid = self.tree.identify_row(event.y)
        if iid:
            self.tree.selection_set(iid)
        menu = tk.Menu(self.tree, tearoff=0)
        menu.add_command(label="Open/View Details", command=self.view_selected_case)
        menu.add_command(label="Edit Selected", command=self.edit_selected_case, accelerator="Enter / Ctrl+E")
        menu.add_command(label="Delete Selected", command=self.delete_selected_cases, accelerator="Del")
        menu.add_command(label="Copy Selected", command=self.copy_selected_treeview_rows, accelerator="Ctrl+C")
        menu.add_separator()
        menu.add_command(label="Duplicate Selected", command=self.duplicate_selected_cases)
        menu.add_separator()
        menu.add_command(label="Export Selected as PDF", command=self.export_selected_pdf)
        menu.add_command(label="Export Selected as XLSX", command=self.export_selected_xlsx)
        menu.tk_popup(event.x_root, event.y_root)

    def focus_find(self):
        try:
            if hasattr(self, 'view_search_entry') and self.view_search_entry:
                self.view_search_entry.focus_set()
                self.view_search_entry.selection_range(0, 'end')
                self.update_status("Type to filter; press Enter to apply.")
            else:
                self.update_status("Search box not available here.")
        except Exception:
            pass

    def view_selected_case(self):
        sel = self.tree.selection()
        if not sel:
            Messagebox.show_info("View", "Select a row to view.")
            return
        item = sel[0]
        values = self.tree.item(item, 'values') or ()
        if not values:
            Messagebox.show_error("View", "Could not read selected row.")
            return
        case_id = values[0]
        case = get_case_by_id_db(case_id)
        if not case:
            Messagebox.show_error("View", f"Case not found: {case_id}")
            return
        win = tk.Toplevel(self.root)
        win.title(f"Case {case.get('case_number') or case_id}")
        win.geometry("560x580")
        frm = ttk.Frame(win, padding=12)
        frm.pack(fill='both', expand=True)
        row = 0
        for key, cfg in self.tree_columns_config.items():
            if key == 'id':
                continue
            label = cfg.get('text', key)
            val = case.get(key, '')
            ttk.Label(frm, text=f"{label}:", width=24).grid(row=row, column=0, sticky='ne', pady=2)
            txt = tk.Text(frm, height=2 if key=='notes' else 1, wrap='word')
            txt.insert('1.0', str(val or ''))
            txt.configure(state='disabled')
            txt.grid(row=row, column=1, sticky='ew', pady=2)
            row += 1
        frm.columnconfigure(1, weight=1)
        ttk.Button(frm, text="Close", command=win.destroy).grid(row=row, column=1, sticky='e', pady=(8,0))

    def duplicate_selected_cases(self):
        sel = self.tree.selection()
        if not sel:
            Messagebox.show_info("Duplicate", "Select one or more rows to duplicate.")
            return
        duplicated = 0
        for item in sel:
            values = self.tree.item(item, 'values') or ()
            if not values:
                continue
            case_id = values[0]
            case = get_case_by_id_db(case_id)
            if not case:
                continue
            original_cn = (case.get('case_number') or '').strip()
            new_cn = self._generate_unique_case_number(original_cn)
            case['case_number'] = new_cn
            # Clean DB-only fields
            case.pop('id', None)
            case.pop('created_at', None)
            if add_case_db(case):
                duplicated += 1
        self.refresh_data_view(reset_lazy=False)
        self.update_status(f"Duplicated {duplicated} case(s).")

    def _generate_unique_case_number(self, base: str) -> str:
        base = base or "Case"
        candidate = f"{base} (copy)"
        n = 2
        try:
            from uuid import uuid4
            while get_case_by_number_db(candidate):
                candidate = f"{base} (copy {n})"
                n += 1
                if n > 50:
                    candidate = f"{base}-{uuid4().hex[:4]}"
                    break
        except Exception:
            pass
        return candidate

    # --- Inline cell editing ---
    def on_treeview_cell_double_click(self, event):
        item = self.tree.identify_row(event.y)
        col = self.tree.identify_column(event.x)  # e.g., '#3'
        if not item or not col:
            return
        # Map column index to column key from displaycolumns
        try:
            col_index = int(col.replace('#', '')) - 1
            display_cols = list(self.tree['displaycolumns'])
            if col_index < 0 or col_index >= len(display_cols):
                return
            col_key = display_cols[col_index]
        except Exception:
            return
        if col_key in ('id', 'created_at'):
            return
        # Start editor
        x, y, w, h = self.tree.bbox(item, col)
        if w <= 0 or h <= 0:
            return
        old_values = self.tree.item(item, 'values') or ()
        try:
            # Find current cell value by index of col_key in self.tree['columns']
            all_cols = list(self.tree['columns'])
            cell_index = all_cols.index(col_key)
            old_value = old_values[cell_index] if cell_index < len(old_values) else ''
        except Exception:
            old_value = ''
        if col_key == 'notes':
            self._open_long_text_editor(item, col_key, old_value)
            return

        # Choose editor type by column
        editor = tb.Entry(self.tree)
        editor.insert(0, str(old_value))

        # Expand editor to improve readability while keeping it on-screen
        try:
            self.tree.update_idletasks()
            tree_width = int(self.tree.winfo_width())
        except Exception:
            tree_width = 0

        try:
            x, y, w, h = int(x), int(y), int(w), int(h)
        except Exception:
            pass

        min_width = max(w, 220)
        width = min(min_width, tree_width) if tree_width else min_width
        x_coord = x
        if tree_width and (x_coord + width) > tree_width:
            x_coord = max(0, tree_width - width)

        editor.place(x=x_coord, y=y, width=width, height=max(h, 24))
        editor.focus_set()
        editor.select_range(0, tk.END)

        self._inline_edit = {'item': item, 'col_key': col_key, 'old': old_value, 'widget': editor}

        def finish(save=True):
            try:
                val = editor.get()
            except Exception:
                val = None
            editor.destroy()
            self._inline_edit = None
            if not save:
                return
            self._apply_treeview_edit(item, col_key, val)

        editor.bind('<Return>', lambda e: finish(True))
        editor.bind('<Escape>', lambda e: finish(False))
        editor.bind('<FocusOut>', lambda e: finish(True))

    def _validate_inline_value(self, col_key: str, value: str):
        if value is None:
            return True, None, None
        v = value.strip()
        # Field-specific rules
        cfg = self.tree_columns_config.get(col_key, {})
        typ = cfg.get('type')
        if col_key in ('start_date', 'end_date', 'created_at') or typ == 'date':
            # Accept MM-DD-YYYY or YYYY-MM-DD; store as YYYY-MM-DD
            from datetime import datetime
            if not v:
                return True, None, None
            fmt = None
            for f in ('%m-%d-%Y', '%Y-%m-%d', '%m/%d/%Y'):
                try:
                    dt = datetime.strptime(v, f)
                    fmt = dt.strftime('%Y-%m-%d')
                    break
                except Exception:
                    continue
            if not fmt:
                return False, None, "Enter date as MM-DD-YYYY."
            return True, fmt, None
        if col_key == 'volume_size_gb' or typ == 'numeric':
            if not v:
                return True, None, None
            try:
                num = float(v)
                return True, num, None
            except Exception:
                return False, None, "Enter a number."
        if col_key == 'fpr_complete' or typ == 'boolean':
            if not v:
                return True, 0, None
            val_true = {'1','true','yes','y','t','on','checked'}
            return True, (1 if v.lower() in val_true else 0), None
        if col_key == 'data_recovered':
            if not v:
                return True, '', None
            val_true = {'yes','y','true','1'}
            return True, ('Yes' if v.lower() in val_true else 'No'), None
        # Default text
        return True, v or None, None

    def _apply_treeview_edit(self, item, col_key, value):
        ok, conv, err = self._validate_inline_value(col_key, value)
        if not ok:
            Messagebox.show_error("Invalid value", err or "Invalid input.")
            return False

        row_vals = self.tree.item(item, 'values') or ()
        if not row_vals:
            return False

        case_id = row_vals[0]
        case = get_case_by_id_db(case_id)
        if not case:
            return False

        old_data = dict(case)
        case[col_key] = conv
        if update_case_db(case_id, case):
            self.push_view_edit_history(old_data, dict(case))
            self.tree.set(item, col_key, conv if conv is not None else '')
            self.update_status("Saved.")
            return True

        Messagebox.show_error("Save failed", "Could not update the case.")
        return False

    def _open_long_text_editor(self, item, col_key, old_value):
        # Provide a larger editor so long text remains visible while editing.
        win = tk.Toplevel(self.root)
        win.title(f"Edit {self.tree_columns_config.get(col_key, {}).get('text', col_key)}")
        win.transient(self.root)
        win.grab_set()

        try:
            win.geometry("520x320")
        except Exception:
            pass

        label = ttk.Label(win, text="Update the value below:")
        label.pack(anchor='w', padx=10, pady=(10, 4))

        text_widget = tk.Text(win, wrap='word')
        text_widget.pack(fill='both', expand=True, padx=10, pady=5)
        text_widget.insert('1.0', str(old_value or ''))
        text_widget.focus_set()

        button_frame = ttk.Frame(win)
        button_frame.pack(fill='x', padx=10, pady=(0, 10))

        def do_save():
            val = text_widget.get('1.0', 'end-1c')
            if self._apply_treeview_edit(item, col_key, val):
                win.destroy()

        def do_cancel():
            win.destroy()

        save_btn = ttk.Button(button_frame, text="Save", command=do_save)
        save_btn.pack(side='right', padx=(6, 0))
        cancel_btn = ttk.Button(button_frame, text="Cancel", command=do_cancel)
        cancel_btn.pack(side='right')

        win.bind('<Control-Return>', lambda e: do_save())
        win.bind('<Escape>', lambda e: do_cancel())

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

        toggles_frame = ttk.Frame(container)
        toggles_frame.pack(fill='x', pady=(0, 5), padx=10)
        self.heatmap_var = tk.BooleanVar(value=self._heatmap_enabled)
        heatmap_cb = ttk.Checkbutton(toggles_frame, text="Show Heatmap", variable=self.heatmap_var, command=self.toggle_heatmap)
        heatmap_cb.pack(side='left')

        # No API key or MapTiler check needed

        # Map widget
        self.map_widget = tkintermapview.TkinterMapView(container, width=800, height=600, corner_radius=0)
        self.map_widget.pack(fill='both', expand=True)

        if not hasattr(self.map_widget, 'set_heatmap'):
            heatmap_cb.configure(state='disabled', text="Heatmap (requires newer map widget)")
            self._heatmap_enabled = False
            self.heatmap_var.set(False)

        # Fit to Locations button row (below map)
        fit_row = ttk.Frame(container)
        fit_row.pack(fill='x', pady=(5, 0), padx=10)
        ttk.Button(fit_row, text="Fit To Locations", command=self.fit_map_to_locations).pack(side='left')

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

        # Hover tooltip bindings on the map canvas
        try:
            self._map_hover_tip = None
            self._map_hover_tip_label = None
            self.map_widget.canvas.bind('<Motion>', self._on_map_mouse_move)
            self.map_widget.canvas.bind('<Leave>', lambda e: self._hide_map_tooltip())
        except Exception:
            pass

    # Removed: _zoom_to_usa and _render_heatmap (heatmap feature dropped)

    def on_marker_click(self, loc):
        """Display marker info for a location with offense types and counts.
        loc: tuple (city, state)
        """
        try:
            city, state = loc
            metrics = self._location_metrics.get((city, state))
            if metrics:
                counts = dict(metrics.get('offense_counts', {}))
                total_cases = metrics.get('total_cases', 0)
                total_volume = metrics.get('total_volume_gb', 0.0)
            else:
                city_cases = self._grouped_cases_by_location.get((city, state), [])
                counts = {}
                total_volume = 0.0
                for c in city_cases:
                    offense = (c.get('offense_type') or '').strip()
                    if offense:
                        counts[offense] = counts.get(offense, 0) + 1
                    total_volume += safe_float_conversion(c.get('volume_size_gb'))
                total_cases = len(city_cases)

            title = f"{city}, {state}"
            volume_text = format_volume_for_display(total_volume)
            if counts:
                items = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
                bullets = [f"• {name}: {n}" for name, n in items]
                total = total_cases if total_cases else sum(counts.values())
                msg = (
                    f"Total cases: {total}\n"
                    f"Total volume: {volume_text}\n\n"
                    + "\n".join(bullets)
                )
            else:
                msg = (
                    f"Total cases: {total_cases}\n"
                    f"Total volume: {volume_text}\n\n"
                    "No offenses recorded"
                )

            # Some environments interpret positional args as (message, title);
            # force correct mapping via keywords.
            # Use Tk's messagebox directly to ensure correct title/body order across themes
            messagebox.showinfo(title=title, message=msg)
        except Exception as e:
            logging.error(f"Error displaying marker info: {e}")
            messagebox.showinfo(title="Location Information", message=f"{loc}")

    def fit_map_to_locations(self):
        """Fit the map viewport to include all current markers."""
        try:
            if not hasattr(self, 'map_markers') or not self.map_markers:
                focal_state = getattr(self, 'map_focal_state_var', None)
                st = focal_state.get() if focal_state else ''
                if st:
                    self.focus_map_on_state(st)
                else:
                    self.map_widget.set_position(32.7, -89.5)
                    self.map_widget.set_zoom(7)
                return

            lats, lons = [], []
            for marker in self.map_markers.values():
                try:
                    lat, lon = marker.position
                except Exception:
                    continue
                lats.append(lat)
                lons.append(lon)
            if not lats or not lons:
                return
            top, bottom = max(lats), min(lats)
            left, right = min(lons), max(lons)
            # Avoid zero-size box
            if abs(top - bottom) < 1e-6:
                top += 0.01
                bottom -= 0.01
            if abs(right - left) < 1e-6:
                right += 0.01
                left -= 0.01
            self.map_widget.fit_bounding_box((top, left), (bottom, right))
        except Exception as e:
            logging.warning(f"fit_map_to_locations fallback: {e}")
            try:
                self.map_widget.set_zoom(6)
            except Exception:
                pass

    def _on_map_mouse_move(self, event):
        """Show a small tooltip near the cursor when hovering close to a marker."""
        try:
            if not hasattr(self, 'map_markers') or not self.map_markers:
                self._hide_map_tooltip()
                return
            # Find nearest marker in screen space
            nearest = None
            nearest_d2 = float('inf')
            zoom = round(self.map_widget.zoom)
            ulx, uly = self.map_widget.upper_left_tile_pos
            lrx, lry = self.map_widget.lower_right_tile_pos
            cw = max(1, self.map_widget.canvas.winfo_width())
            ch = max(1, self.map_widget.canvas.winfo_height())

            for (city, state), marker in self.map_markers.items():
                try:
                    mlat, mlon = marker.position
                except Exception:
                    continue
                try:
                    mtx, mty = decimal_to_osm(mlat, mlon, zoom)
                    rx = (mtx - ulx) / (lrx - ulx) if lrx != ulx else 0.5
                    ry = (mty - uly) / (lry - uly) if lry != uly else 0.5
                    mx = int(rx * cw)
                    my = int(ry * ch)
                except Exception:
                    continue
                dx = mx - event.x
                dy = my - event.y
                d2 = dx*dx + dy*dy
                if d2 < nearest_d2:
                    nearest = (city, state)
                    nearest_d2 = d2

            # 18px radius threshold
            if nearest is None or nearest_d2 > (18*18):
                self._hide_map_tooltip()
                return

            city, state = nearest
            n = len(self._grouped_cases_by_location.get((city, state), [])) if hasattr(self, '_grouped_cases_by_location') else 0
            text = f"{city}, {state} — {n} case{'s' if n != 1 else ''}"
            self._show_map_tooltip(event.x_root, event.y_root, text)
        except Exception:
            self._hide_map_tooltip()

    def _show_map_tooltip(self, x_root, y_root, text):
        try:
            if getattr(self, '_map_hover_tip', None) is None:
                tip = tk.Toplevel(self.root)
                tip.wm_overrideredirect(True)
                try:
                    tip.attributes('-topmost', True)
                except Exception:
                    pass
                frame = ttk.Frame(tip, padding=(6, 3))
                frame.pack(fill='both', expand=True)
                lbl = ttk.Label(frame, text=text)
                lbl.pack()
                self._map_hover_tip = tip
                self._map_hover_tip_label = lbl
            else:
                self._map_hover_tip_label.config(text=text)
            self._map_hover_tip.geometry(f"+{x_root + 12}+{y_root + 12}")
            self._map_hover_tip.deiconify()
        except Exception:
            pass

    def _hide_map_tooltip(self):
        try:
            if getattr(self, '_map_hover_tip', None) is not None:
                self._map_hover_tip.withdraw()
        except Exception:
            pass

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
                "Examiner", "Investigator", "Forensic Tool", "Year", "City of Offense", "Total Volume (GB/TB)",
                "Total Volume by Examiner", "Total Volume by Investigator", "Total Volume by Agency", "Total Volume by Device Type",
                "Total Volume by Forensic Tool"
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

        ttk.Label(controls_frame, text="Chart Style:").pack(side='left', padx=(0, 5))
        self.graph_style_var = tk.StringVar(value="Bar")
        self.graph_style_combo = ttk.Combobox(
            controls_frame,
            textvariable=self.graph_style_var,
            values=["Bar", "Stacked Bar", "Pie", "Line"],
            state="readonly",
            width=12
        )
        self.graph_style_combo.pack(side='left', padx=(0, 10))
        self.graph_style_combo.bind("<<ComboboxSelected>>", lambda e: self.update_graph())


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
        if not (self.ax and self.fig and self.canvas_agg):
            return

        # Reset axes so previous chart state (like pie equal-aspect) doesn't shrink other plots
        try:
            self.ax.set_aspect('auto')
        except Exception:
            pass

        graph_type = self.graph_type_var.get()
        year_filter = self.graph_year_var.get()
        style = getattr(self, 'graph_style_var', None)
        style_value = style.get() if isinstance(style, tk.StringVar) else "Bar"

        completed_cases = get_all_cases_db()
        in_progress_cases = get_all_in_progress_cases_db()

        def filter_cases_by_year(items):
            if year_filter and year_filter != "All":
                filtered = []
                for case in items:
                    start = (case.get("start_date") or "")
                    if start and start.startswith(year_filter):
                        filtered.append(case)
                return filtered
            return items

        completed_cases = filter_cases_by_year(completed_cases)
        in_progress_cases = filter_cases_by_year(in_progress_cases)
        all_cases = completed_cases + in_progress_cases

        def collect_counts(cases_list, extractor):
            counts = {}
            for case in cases_list:
                key = extractor(case)
                counts[key] = counts.get(key, 0) + 1
            return counts

        def collect_volume(cases_list, field):
            totals = {}
            for case in cases_list:
                key = (case.get(field) or "Unknown").strip() or "Unknown"
                totals[key] = totals.get(key, 0.0) + safe_float_conversion(case.get("volume_size_gb"))
            return totals

        def sort_keys_by_total(keys, completed_map, in_progress_map):
            def total_for(key):
                return completed_map.get(key, 0) + in_progress_map.get(key, 0)
            return sorted(keys, key=lambda k: (-total_for(k), k.lower()))

        # Handle total volume by groupings
        group_volume_types = {
            "Total Volume by Examiner": "examiner",
            "Total Volume by Investigator": "investigator",
            "Total Volume by Agency": "agency",
            "Total Volume by Device Type": "device_type",
            "Total Volume by Forensic Tool": "forensic_tool",
        }

        if graph_type in group_volume_types:
            group_field = group_volume_types[graph_type]
            completed_totals = collect_volume(completed_cases, group_field)
            in_progress_totals = collect_volume(in_progress_cases, group_field)
            combined_keys = sort_keys_by_total(set(completed_totals) | set(in_progress_totals), completed_totals, in_progress_totals)
            if not combined_keys:
                self.ax.clear()
                self.ax.text(0.5, 0.5, "No data to display", ha='center', va='center', fontsize=16)
                self.canvas_agg.draw()
                return

            combined_totals = {k: completed_totals.get(k, 0.0) + in_progress_totals.get(k, 0.0) for k in combined_keys}
            use_tb = any(val > 999 for val in combined_totals.values())
            def convert(value):
                return value / 1024.0 if use_tb else value

            if style_value == "Stacked Bar":
                completed_values = [convert(completed_totals.get(k, 0.0)) for k in combined_keys]
                in_progress_values = [convert(in_progress_totals.get(k, 0.0)) for k in combined_keys]
                completed_display = [format_volume_for_display(completed_totals.get(k, 0.0)) for k in combined_keys]
                in_progress_display = [format_volume_for_display(in_progress_totals.get(k, 0.0)) for k in combined_keys]

                self.ax.clear()
                bars_completed = self.ax.bar(combined_keys, completed_values, label="Completed", color="#4a90e2")
                bars_in_progress = self.ax.bar(
                    combined_keys,
                    in_progress_values,
                    bottom=completed_values,
                    label="In Progress",
                    color="#f5a623"
                )
                totals_display = [format_volume_for_display(combined_totals[k]) for k in combined_keys]
                for idx, key in enumerate(combined_keys):
                    stacked_height = completed_values[idx] + in_progress_values[idx]
                    if stacked_height <= 0:
                        continue
                    self.ax.text(idx, stacked_height, totals_display[idx], ha='center', va='bottom', fontsize=9)
                for bar, val in zip(bars_completed, completed_display):
                    if bar.get_height() > 0:
                        self.ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()/2, val, ha='center', va='center', fontsize=8, color='white')
                for bar, base, val in zip(bars_in_progress, completed_values, in_progress_display):
                    if bar.get_height() > 0:
                        self.ax.text(bar.get_x() + bar.get_width()/2, base + bar.get_height()/2, val, ha='center', va='center', fontsize=8, color='black')
                self.ax.set_ylabel("Total Volume (TB)" if use_tb else "Total Volume (GB)")
                self.ax.set_xlabel(group_field.replace('_', ' ').title())
                self.ax.set_title(graph_type)
                self.ax.legend()
                self.ax.tick_params(axis='x', rotation=45)
                self.fig.autofmt_xdate(rotation=45)
                self.fig.subplots_adjust(bottom=0.27)
                self.fig.tight_layout()
                self.canvas_agg.draw()
                return

            combined_values = [convert(combined_totals[k]) for k in combined_keys]
            combined_display = [format_volume_for_display(combined_totals[k]) for k in combined_keys]

            if style_value == "Pie":
                if not any(combined_totals[k] > 0 for k in combined_keys):
                    self.ax.clear()
                    self.ax.text(0.5, 0.5, "No data to display", ha='center', va='center', fontsize=16)
                    self.canvas_agg.draw()
                    return
                self.ax.clear()
                try:
                    self.ax.set_aspect('equal')
                except Exception:
                    pass
                self.ax.pie(
                    combined_values,
                    labels=combined_keys,
                    autopct='%1.1f%%',
                    startangle=90,
                    pctdistance=0.8
                )
                self.ax.set_title(graph_type)
                self.fig.tight_layout()
                self.canvas_agg.draw()
                return

            if style_value == "Line":
                self.ax.clear()
                self.ax.plot(range(len(combined_keys)), combined_values, marker='o', color="#4a90e2")
                self.ax.set_xticks(range(len(combined_keys)))
                self.ax.set_xticklabels(combined_keys, rotation=45, ha='right')
                self.ax.set_ylabel("Total Volume (TB)" if use_tb else "Total Volume (GB)")
                self.ax.set_xlabel(group_field.replace('_', ' ').title())
                self.ax.set_title(graph_type)
                for idx, value in enumerate(combined_values):
                    if value <= 0:
                        continue
                    self.ax.text(idx, value, combined_display[idx], ha='center', va='bottom', fontsize=9)
                self.fig.subplots_adjust(bottom=0.27)
                self.fig.tight_layout()
                self.canvas_agg.draw()
                return

            # Default bar chart for volume
            self.ax.clear()
            bars = self.ax.bar(combined_keys, combined_values, color="#4a90e2")
            self.ax.set_xlabel(group_field.replace('_', ' ').title())
            self.ax.set_ylabel("Total Volume (TB)" if use_tb else "Total Volume (GB)")
            self.ax.set_title(graph_type)
            self.ax.tick_params(axis='x', rotation=45)
            self.fig.autofmt_xdate(rotation=45)
            for bar, label in zip(bars, combined_display):
                if bar.get_height() > 0:
                    self.ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), label, ha='center', va='bottom', fontsize=9)
            self.fig.subplots_adjust(bottom=0.27)
            self.fig.tight_layout()
            self.canvas_agg.draw()
            return

        if graph_type == "Total Volume (GB/TB)":
            total_gb = sum(safe_float_conversion(case.get("volume_size_gb")) for case in all_cases)
            if total_gb <= 0:
                self.ax.clear()
                self.ax.text(0.5, 0.5, "No volume recorded", ha='center', va='center', fontsize=16)
                self.canvas_agg.draw()
                return
            if total_gb > 999:
                total_val = total_gb / 1024.0
                y_label = "Total Volume (TB)"
                display_value = f"{total_val:.2f} TB"
            else:
                total_val = total_gb
                y_label = "Total Volume (GB)"
                display_value = f"{total_val:.2f} GB"

            self.ax.clear()
            self.ax.bar(["Total"], [total_val], color="#4a90e2")
            self.ax.set_ylabel(y_label)
            self.ax.set_title("Total Volume of All Cases")
            self.ax.text(0, total_val, display_value, ha='center', va='bottom', fontsize=14, fontweight='bold')
            self.fig.tight_layout()
            self.canvas_agg.draw()
            return

        # Map graph type to DB field for count-based charts
        graph_field_map = {
            "Offense Type": "offense_type",
            "Device Type": "device_type",
            "OS": "os",
            "Agency": "agency",
            "State of Offense": "state_of_offense",
            "Examiner": "examiner",
            "Investigator": "investigator",
            "Forensic Tool": "forensic_tool",
            "Year": "start_date",
            "City of Offense": "city_of_offense",
        }

        def year_extractor(case):
            for key in ("start_date", "created_at"):
                value = case.get(key)
                if value and len(str(value)) >= 4:
                    year = str(value)[:4]
                    if year.isdigit():
                        return year
            return "Unknown"

        if graph_type == "Year":
            current_extractor = year_extractor
            xlabel = "Year"
        else:
            field = graph_field_map.get(graph_type, "offense_type")

            def value_extractor(case, field_name=field):
                value = (case.get(field_name) or "Unknown").strip()
                return value if value else "Unknown"

            current_extractor = value_extractor
            xlabel = graph_type

        completed_counts = collect_counts(completed_cases, current_extractor)
        in_progress_counts = collect_counts(in_progress_cases, current_extractor)
        combined_keys = sort_keys_by_total(set(completed_counts) | set(in_progress_counts), completed_counts, in_progress_counts)

        if not combined_keys:
            self.ax.clear()
            self.ax.text(0.5, 0.5, "No data to display", ha='center', va='center', fontsize=16)
            self.canvas_agg.draw()
            return

        if style_value == "Stacked Bar":
            completed_values = [completed_counts.get(k, 0) for k in combined_keys]
            in_progress_values = [in_progress_counts.get(k, 0) for k in combined_keys]
            totals = [completed_values[i] + in_progress_values[i] for i in range(len(combined_keys))]

            self.ax.clear()
            bars_completed = self.ax.bar(combined_keys, completed_values, label="Completed", color="#4a90e2")
            bars_in_progress = self.ax.bar(
                combined_keys,
                in_progress_values,
                bottom=completed_values,
                label="In Progress",
                color="#f5a623"
            )
            for idx, total in enumerate(totals):
                if total > 0:
                    self.ax.text(idx, total, str(total), ha='center', va='bottom', fontsize=9)
            self.ax.set_ylabel("Count")
            self.ax.set_xlabel(xlabel)
            self.ax.set_title(f"{graph_type} Distribution")
            self.ax.legend()
            self.ax.tick_params(axis='x', rotation=45)
            self.fig.autofmt_xdate(rotation=45)
            self.fig.subplots_adjust(bottom=0.27)
            self.fig.tight_layout()
            self.canvas_agg.draw()
            return

        combined_counts = {k: completed_counts.get(k, 0) + in_progress_counts.get(k, 0) for k in combined_keys}
        combined_values = [combined_counts[k] for k in combined_keys]

        if style_value == "Pie":
            if not any(combined_counts[k] > 0 for k in combined_keys):
                self.ax.clear()
                self.ax.text(0.5, 0.5, "No data to display", ha='center', va='center', fontsize=16)
                self.canvas_agg.draw()
                return
            self.ax.clear()
            try:
                self.ax.set_aspect('equal')
            except Exception:
                pass
            self.ax.pie(
                combined_values,
                labels=combined_keys,
                autopct='%1.1f%%',
                startangle=90,
                pctdistance=0.8
            )
            self.ax.set_title(f"{graph_type} Distribution")
            self.fig.tight_layout()
            self.canvas_agg.draw()
            return

        if style_value == "Line":
            self.ax.clear()
            self.ax.plot(range(len(combined_keys)), combined_values, marker='o', color="#4a90e2")
            self.ax.set_xticks(range(len(combined_keys)))
            self.ax.set_xticklabels(combined_keys, rotation=45, ha='right')
            self.ax.set_ylabel("Count")
            self.ax.set_xlabel(xlabel)
            self.ax.set_title(f"{graph_type} Distribution")
            for idx, value in enumerate(combined_values):
                if value > 0:
                    self.ax.text(idx, value, str(value), ha='center', va='bottom', fontsize=9)
            self.fig.subplots_adjust(bottom=0.27)
            self.fig.tight_layout()
            self.canvas_agg.draw()
            return

        # Default bar chart for counts
        self.ax.clear()
        bars = self.ax.bar(combined_keys, combined_values, color="#4a90e2")
        self.ax.set_xlabel(xlabel)
        self.ax.set_ylabel("Count")
        self.ax.set_title(f"{graph_type} Distribution")
        self.ax.tick_params(axis='x', rotation=45)
        self.fig.autofmt_xdate(rotation=45)
        for bar, value in zip(bars, combined_values):
            if value > 0:
                self.ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), str(value), ha='center', va='bottom', fontsize=9)
        self.fig.subplots_adjust(bottom=0.27)
        self.fig.tight_layout()
        self.canvas_agg.draw()

    def create_settings_widgets(self):
        # """Creates the widgets for the Settings tab."""
        self.settings_frame.rowconfigure(0, weight=1)
        self.settings_frame.columnconfigure(0, weight=1)
        settings_content_frame = ttk.Frame(self.settings_frame)
        settings_content_frame.pack(fill='both', expand=True)
        # --- Auto Update Check Option ---
        auto_update_var = tk.BooleanVar(value=get_user_pref('auto_update_check', True))
        def on_auto_update_toggle():
            set_user_pref('auto_update_check', auto_update_var.get())
        auto_update_chk = ttk.Checkbutton(settings_content_frame, text="Check for updates on launch", variable=auto_update_var, command=on_auto_update_toggle)
        auto_update_chk.pack(anchor='w', padx=10, pady=(8, 0))

        # --- Make the Settings content scrollable (for small screens) ---
        # Create a canvas + vertical scrollbar, and an inner frame that actually holds content
        _canvas = tk.Canvas(settings_content_frame, borderwidth=0, highlightthickness=0)
        _vscroll = ttk.Scrollbar(settings_content_frame, orient='vertical', command=_canvas.yview)
        _canvas.configure(yscrollcommand=_vscroll.set)
        _canvas.pack(side='left', fill='both', expand=True)
        _vscroll.pack(side='right', fill='y')

        # Inner frame to pack all settings sections into
        _inner = ttk.Frame(_canvas)
        _win = _canvas.create_window((0, 0), window=_inner, anchor='nw')

        def _on_frame_configure(event=None):
            try:
                _canvas.configure(scrollregion=_canvas.bbox('all'))
            except Exception:
                pass
        _inner.bind('<Configure>', _on_frame_configure)

        def _on_canvas_configure(event):
            try:
                _canvas.itemconfigure(_win, width=event.width)
            except Exception:
                pass
        _canvas.bind('<Configure>', _on_canvas_configure)

        # Mouse wheel scroll support (Windows)
        def _on_mousewheel(event):
            try:
                delta = -int(event.delta/120)
                _canvas.yview_scroll(delta, 'units')
            except Exception:
                pass
        def _bind_wheel(w):
            w.bind('<Enter>', lambda e: w.bind_all('<MouseWheel>', _on_mousewheel))
            w.bind('<Leave>', lambda e: w.unbind_all('<MouseWheel>'))
        _bind_wheel(_inner)
        _bind_wheel(_canvas)

        # Rebind variable so existing code packs into the scrollable content
        settings_content_frame = _inner


        # --- Map Marker Icon Section (Single, Optimized) ---
        marker_icon_section_frame = ttk.Frame(settings_content_frame)
        # --- Map Marker Icon Section (Single, Optimized) ---
        marker_icon_section_frame = ttk.Frame(settings_content_frame)
        marker_icon_section_frame.pack(fill='x', pady=10, anchor='w', padx=10)
        ttk.Label(marker_icon_section_frame, text="Map Marker Icon:", font=("Segoe UI", 10, "bold")).pack(anchor='w', pady=(0, 5))
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
        ttk.Label(logo_section_frame, text="Header Logo:", font=("Segoe UI", 10, "bold")).pack(anchor='w', pady=(0, 5))
        ttk.Label(logo_section_frame, text=f"Select image (png, jpg, jpeg, gif).\nSaved as logo.png in:\n{DATA_DIR}").pack(anchor='w', pady=(0, 10))
        select_logo_button_frame = ttk.Frame(logo_section_frame)
        select_logo_button_frame.pack(fill='x', pady=5, anchor='w')
        select_button = ttk.Button(select_logo_button_frame, text="Select Logo File...", command=self.select_logo)
        select_button.pack(side='left')
        # Canvas for logo preview
        self.logo_preview_canvas = tk.Canvas(logo_section_frame, width=200, height=100, bg="lightgrey", relief="sunken")
        self.logo_preview_canvas.pack(pady=10, anchor='w')
        # Initial preview update is now called in __init__ after load_logo_image
        # Ensure previews reflect persisted files now that canvases exist
        try:
            self.load_marker_icon_image()
        except Exception:
            pass
        try:
            self.load_logo_image()
        except Exception:
            pass

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

        # --- Backup & Restore Section ---
        backup_section_frame = ttk.Frame(settings_content_frame)
        backup_section_frame.pack(fill='x', pady=10, anchor='w', padx=10)
        ttk.Label(backup_section_frame, text="Backup & Restore:", font=("Segoe UI", 10, "bold")).pack(anchor='w', pady=(0, 5))
        ttk.Label(backup_section_frame, text=f"Backups are stored in:\n{BACKUP_DIR}").pack(anchor='w', pady=(0, 5))
        backup_buttons_frame = ttk.Frame(backup_section_frame)
        backup_buttons_frame.pack(fill='x', pady=5, anchor='w')
        ttk.Button(backup_buttons_frame, text="Backup Now", command=self.backup_database_now).pack(side='left', padx=(0,5))
        ttk.Button(backup_buttons_frame, text="Restore from File…", command=self.restore_database_from_file).pack(side='left', padx=(0,5))
        ttk.Button(backup_buttons_frame, text="Open Backups Folder", command=self.open_backups_folder).pack(side='left', padx=(0,5))

        # --- Health Panel ---
        health_section = ttk.Frame(settings_content_frame)
        health_section.pack(fill='x', pady=10, anchor='w', padx=10)
        ttk.Label(health_section, text="Database Health:", font=("Segoe UI", 10, "bold")).pack(anchor='w', pady=(0, 5))
        self.health_text = tk.StringVar(value="")
        health_label = ttk.Label(health_section, textvariable=self.health_text, justify='left')
        health_label.pack(anchor='w')
        btns = ttk.Frame(health_section)
        btns.pack(fill='x', pady=(6,0))
        def refresh_health():
            s = get_database_health_stats()
            kb = s['db_size_bytes']/1024 if s['db_size_bytes'] else 0
            mb = kb/1024
            last = s.get('last_backup') or 'None'
            txt = (
                f"DB: {s.get('db_path','')}\n"
                f"Size: {mb:.2f} MB ({int(kb)} KB)\n"
                f"Rows - Cases: {s.get('case_log_rows',0)}, In-Progress: {s.get('in_progress_rows',0)}, Geocache: {s.get('geocache_rows',0)}\n"
                f"Geocache Hits: {s.get('geocache_hits',0)}, Misses: {s.get('geocache_misses',0)}\n"
                f"Last Backup: {last}"
            )
            self.health_text.set(txt)
        ttk.Button(btns, text="Refresh", command=refresh_health).pack(side='left', padx=(0,6))
        def optimize_now():
            try:
                self.update_status("Optimizing database…")
                optimize_database()
                refresh_health()
                self.update_status("Database optimized.", duration=3000)
            except Exception as e:
                Messagebox.show_error("Optimize Failed", str(e))
        ttk.Button(btns, text="Optimize (VACUUM/ANALYZE)", command=optimize_now).pack(side='left')
        # Initial populate
        refresh_health()

        # Display default password and warning
        password_warning_label = ttk.Label(settings_content_frame,
                           text=f"Default Password: {DEFAULT_PASSWORD}\n(It is highly recommended to change the default password for security.)",
                           foreground="black")
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
        # Build dynamic list of available themes from ttkbootstrap if possible
        try:
            if hasattr(self.root, 'style') and hasattr(self.root.style, 'theme_names'):
                available_themes = list(self.root.style.theme_names())
            elif hasattr(self, 'style') and hasattr(self.style, 'theme_names'):
                available_themes = list(self.style.theme_names())
            else:
                # Fallback to known options if style is unavailable
                available_themes = [code for _, code in THEME_OPTIONS]
        except Exception:
            available_themes = [code for _, code in THEME_OPTIONS]

        # Deduplicate and sort for a stable list
        theme_names = sorted(set(available_themes), key=str.lower)

        # Set combobox to match saved or current theme
        saved_theme_code = getattr(self, '_saved_theme_code', None)
        if saved_theme_code in theme_names:
            self.theme_var.set(saved_theme_code)
        else:
            current_theme_code = None
            if hasattr(self.root, 'style') and hasattr(self.root.style, 'theme'):
                try:
                    current_theme_code = self.root.style.theme.name
                except Exception:
                    current_theme_code = None
            if current_theme_code in theme_names:
                self.theme_var.set(current_theme_code)
            elif theme_names:
                self.theme_var.set(theme_names[0])

        theme_combo = ttk.Combobox(theme_section_frame, textvariable=self.theme_var, values=theme_names, state="readonly", width=20)
        theme_combo.pack(anchor='w', pady=(0, 5))

        def on_theme_change(event=None):
            selected_code = self.theme_var.get()
            # Apply theme using ttkbootstrap style if present
            if hasattr(self.root, 'style'):
                self.root.style.theme_use(selected_code)
            else:
                try:
                    self.style.theme_use(selected_code)
                except Exception:
                    pass
            set_user_pref('theme', selected_code)
            self._saved_theme_code = selected_code
            # Refresh contrast-aware labels immediately so values flip black/white without restart
            try:
                self.root.after_idle(self.refresh_contrast_colors)
            except Exception:
                try:
                    self.refresh_contrast_colors()
                except Exception:
                    pass

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


        # --- Insert or Update based on editing state ---
        if self.editing_in_progress_case_id is not None:
            # We are editing an existing in-progress case
            case_id_to_update = self.editing_in_progress_case_id
            logging.info(f"Attempting to update in-progress case ID: {case_id_to_update}")

            if update_in_progress_case_db(case_id_to_update, case_data):
                Messagebox.show_info("Success", f"In-progress case ID {case_id_to_update} updated successfully.")
                logging.info(f"In-progress case ID {case_id_to_update} updated.")
                self.clear_entry_form() # Clear form and reset editing state
                self.refresh_in_progress_view() # Refresh the in-progress view to show changes
                self.update_status(f"In-progress case ID {case_id_to_update} updated.")
            else:
                Messagebox.show_error("Database Error", f"Failed to update in-progress case ID {case_id_to_update}. See log for details.")
                self.update_status(f"Failed to update in-progress case ID {case_id_to_update}.")

        elif self.editing_case_id is not None:
            # We are editing an existing completed case
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
        for key in ["examiner", "investigator", "agency", "offense_type", "city_of_offense", "forensic_tool"]:
            if key in self.entries and isinstance(self.entries[key], tk.StringVar):
                value = self.entries[key].get().strip()
                if value:
                    values = get_combo_values_db(key)
                    if value not in values:
                        values.append(value)
                        set_combo_values_db(key, values)

        # No matter if insert or update, refresh related parts of the UI
        # Already done within the if/else blocks above

    def submit_in_progress_case(self):
        """Collects data from the entry form and adds it as an in-progress case."""
        case_data = self.collect_form_data(for_validation=True)

        # --- Validation ---
        case_number = case_data.get("case_number", "").strip()

        # Validate and convert volume_size_gb to float or None
        vol_size_str = case_data.get('volume_size_gb', '').strip()
        if vol_size_str:
             try:
                 case_data['volume_size_gb'] = float(vol_size_str)
             except ValueError:
                 Messagebox.show_info("Validation Error", "Volume Size (GB) must be a valid number.")
                 logging.warning(f"In-progress submit failed: Invalid Volume Size (GB) '{vol_size_str}'.")
                 return # Stop if invalid number
        else:
             case_data['volume_size_gb'] = None # Store as None if empty

        # Handle 'data_recovered' - it comes as boolean from the checkbox now
        dr_val = case_data.get('data_recovered')
        if dr_val is True:
            case_data['data_recovered'] = "Yes"
        elif dr_val is False:
            case_data['data_recovered'] = "No"
        else:
            case_data['data_recovered'] = ""

        # --- Add or Update in-progress case ---
        if self.editing_in_progress_case_id is not None:
            # We are editing an existing in-progress case
            case_id_to_update = self.editing_in_progress_case_id
            logging.info(f"Attempting to update in-progress case ID: {case_id_to_update}")
            
            if update_in_progress_case_db(case_id_to_update, case_data):
                Messagebox.show_info("Success", f"In-progress case ID {case_id_to_update} updated successfully.")
                logging.info(f"In-progress case ID {case_id_to_update} updated.")
                self.clear_entry_form() # Clear form and reset editing state
                self.refresh_in_progress_view() # Refresh the in-progress view to show changes
                self.update_status(f"In-progress case ID {case_id_to_update} updated.")
            else:
                Messagebox.show_error("Database Error", f"Failed to update in-progress case ID {case_id_to_update}. See log for details.")
                self.update_status(f"Failed to update in-progress case ID {case_id_to_update}.")
        else:
            # We are adding a new in-progress case
            logging.info(f"Attempting to submit new in-progress case: {case_number}")
            if add_in_progress_case_db(case_data):
                Messagebox.show_info("Success", "Case added to In Progress successfully.")
                logging.info(f"New in-progress case '{case_number}' submitted.")
                self.clear_entry_form() # Clear form after successful submission
                self.refresh_in_progress_view() # Refresh the in-progress view to show the new case
                self.update_status(f"New in-progress case '{case_number}' submitted.")
            else:
                Messagebox.show_error("Database Error", f"Failed to submit in-progress case '{case_number}'. See log for details.")
                self.update_status(f"Failed to submit in-progress case '{case_number}'.")

        # Update combo values for persistent fields
        for key in ["examiner", "investigator", "agency", "offense_type", "city_of_offense", "forensic_tool"]:
            if key in self.entries and isinstance(self.entries[key], tk.StringVar):
                value = self.entries[key].get().strip()
                if value:
                    values = get_combo_values_db(key)
                    if value not in values:
                        values.append(value)
                        set_combo_values_db(key, values)


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

    # --- In-Progress Cases Methods ---
    
    def refresh_in_progress_view(self, filter_text=None, priority_filter=None):
        """Refresh the in-progress treeview with optional text and priority filtering."""
        if not hasattr(self, 'in_progress_tree') or not self.in_progress_tree:
            return

        self.in_progress_tree.delete(*self.in_progress_tree.get_children())

        cases = get_all_in_progress_cases_db()

        # Apply text filter
        if filter_text:
            filter_text = filter_text.lower().strip()
            filtered_cases = []
            for case in cases:
                case_str = ' '.join(str(v) for v in case.values() if v is not None).lower()
                if filter_text in case_str:
                    filtered_cases.append(case)
            cases = filtered_cases

        # Apply priority filter
        if priority_filter:
            priority_filtered_cases = []
            for case in cases:
                if case.get('priority', '').lower() == priority_filter.lower():
                    priority_filtered_cases.append(case)
            cases = priority_filtered_cases

        # Insert rows into the treeview
        columns = list(self.in_progress_tree_columns_config.keys())
        for case in cases:
            values = [case.get(col, "") for col in columns]
            self.in_progress_tree.insert("", "end", values=values)

        # Update dashboard summary
        if hasattr(self, 'update_dashboard_summary'):
            self.update_dashboard_summary()
    
    def apply_in_progress_filter(self):
        """Apply search and priority filters to in-progress cases."""
        filter_text = self.in_progress_search_var.get() if hasattr(self, 'in_progress_search_var') else None
        priority_filter = self.priority_filter_var.get() if hasattr(self, 'priority_filter_var') else None
        # Convert 'All' to None for no priority filtering
        if priority_filter == 'All':
            priority_filter = None
        self.refresh_in_progress_view(filter_text, priority_filter)
    
    def clear_in_progress_filter(self):
        """Clear search and priority filters for in-progress cases."""
        if hasattr(self, 'in_progress_search_var'):
            self.in_progress_search_var.set("")
        if hasattr(self, 'priority_filter_var'):
            self.priority_filter_var.set("All")
        self.refresh_in_progress_view()
    
    def edit_selected_in_progress_case(self):
        """Edit the selected in-progress case in the New Entry tab."""
        if not self.in_progress_tree or not self.in_progress_tree.selection():
            Messagebox.show_info("No Selection", "Please select an in-progress case to edit.")
            return
        selected_item = self.in_progress_tree.selection()[0]
        # Get the case ID from the first column (id column) which is hidden
        case_id = self.in_progress_tree.item(selected_item)['values'][0]
        
        if not case_id:
            Messagebox.show_error("Error", "Could not retrieve case ID.")
            return
        
        case = get_in_progress_case_by_id_db(case_id)
        if not case:
            Messagebox.show_error("Error", f"In-progress case with ID {case_id} not found.")
            return
        
        # Set editing state for in-progress case
        self.editing_in_progress_case_id = case_id
        self.editing_case_id = None  # Clear regular editing state
        
        # Populate the form
        self.populate_entry_form(case)
        
        # Update button text and tab title
        if self.submit_button:
            self.submit_button.config(text="Update In-Progress", style="Warning.TButton")
        if self.in_progress_button:
            self.in_progress_button.config(text="Save Changes", style="Accent.TButton")
        
        # Switch to entry tab
        self.notebook.select(self.entry_frame)
        if hasattr(self, 'notebook') and hasattr(self, 'entry_frame'):
            self.notebook.tab(self.entry_frame, text="Edit In-Progress Case")
    
    def mark_case_as_completed(self):
        """Move selected in-progress case to completed cases."""
        if not self.in_progress_tree or not self.in_progress_tree.selection():
            Messagebox.show_info("No Selection", "Please select an in-progress case to mark as completed.")
            return
        selected_items = self.in_progress_tree.selection()
        if len(selected_items) > 1:
            Messagebox.show_info("Multiple Selection", "Please select only one case to mark as completed.")
            return
        selected_item = selected_items[0]
        # Get the case ID from the first column (id column) which is hidden
        case_id = self.in_progress_tree.item(selected_item)['values'][0]
        
        if not case_id:
            Messagebox.show_error("Error", "Could not retrieve case ID.")
            return
        
        # Confirm action
        case = get_in_progress_case_by_id_db(case_id)
        case_number = case.get('case_number', 'Unknown') if case else 'Unknown'
        
        confirm = messagebox.askyesno(
            "Confirm Completion", 
            f"Mark case '{case_number}' as completed?\n\nThis will move it from In Progress to the completed cases list."
        )
        
        if confirm:
            if move_case_to_completed(case_id):
                Messagebox.show_info("Success", f"Case '{case_number}' marked as completed.")
                self.refresh_in_progress_view()
                self.refresh_data_view()  # Refresh main view to show the completed case
                # Refresh map markers to reflect updated city offense aggregations
                try:
                    self.load_map_markers()
                except Exception as e:
                    logging.warning(f"Map refresh failed after completion: {e}")
                self.update_status(f"Case '{case_number}' marked as completed.")
            else:
                Messagebox.show_error("Error", f"Failed to mark case '{case_number}' as completed.")
    
    def delete_selected_in_progress_cases(self):
        """Delete selected in-progress cases after confirmation."""
        if not self.in_progress_tree or not self.in_progress_tree.selection():
            Messagebox.show_info("No Selection", "Please select in-progress cases to delete.")
            return
        selected_items = self.in_progress_tree.selection() if self.in_progress_tree else []
        case_numbers = []
        case_ids = []
        for item in selected_items:
            case_id = self.in_progress_tree.item(item)['values'][0]
            if case_id:
                case_ids.append(case_id)
                case = get_in_progress_case_by_id_db(case_id)
                if case:
                    case_numbers.append(case.get('case_number', f'ID:{case_id}'))
        if not case_ids:
            Messagebox.show_error("Error", "Could not retrieve case IDs.")
            return
        cases_text = ', '.join(case_numbers)
        confirm = messagebox.askyesno(
            "Confirm Deletion",
            f"Are you sure you want to delete the following in-progress cases?\n\n{cases_text}\n\nThis action cannot be undone."
        )
        if confirm:
            deleted_count = 0
            for case_id in case_ids:
                if delete_in_progress_case_db(case_id):
                    deleted_count += 1
            if deleted_count > 0:
                Messagebox.show_info("Success", f"Deleted {deleted_count} in-progress case(s).")
                self.refresh_in_progress_view()
                self.update_status(f"Deleted {deleted_count} in-progress case(s).")
            else:
                Messagebox.show_error("Error", "Failed to delete in-progress cases.")

    # === Phase 2: Bulk Operations ===
    
    def bulk_set_priority(self):
        """Set priority for multiple selected in-progress cases."""
        if not self.in_progress_tree or not self.in_progress_tree.selection():
            Messagebox.show_info("No Selection", "Please select in-progress cases to update priority.")
            return
        selected_items = self.in_progress_tree.selection() if self.in_progress_tree else []
        dialog = tk.Toplevel(self.root)
        dialog.title("Bulk Set Priority")
        dialog.geometry("300x150")
        dialog.transient(self.root)
        dialog.grab_set()
        ttk.Label(dialog, text="Select new priority for selected cases:").pack(pady=10)
        priority_var = tk.StringVar(value='Medium')
        priority_combo = ttk.Combobox(dialog, textvariable=priority_var, 
                                    values=['Critical', 'High', 'Medium', 'Low'], 
                                    state='readonly')
        priority_combo.pack(pady=10)
        def apply_bulk_priority():
            new_priority = priority_var.get()
            updated_count = 0
            for item in selected_items:
                case_id = self.in_progress_tree.item(item)['values'][0]
                if case_id:
                    update_data = {'priority': new_priority}
                    if update_in_progress_case_db(case_id, update_data):
                        updated_count += 1
            dialog.destroy()
            if updated_count > 0:
                Messagebox.show_info("Success", f"Updated priority for {updated_count} case(s) to '{new_priority}'.")
                self.refresh_in_progress_view()
            else:
                Messagebox.show_error("Error", "Failed to update case priorities.")
        ttk.Button(dialog, text="Apply", command=apply_bulk_priority).pack(pady=10)
        ttk.Button(dialog, text="Cancel", command=dialog.destroy).pack()
    
    def bulk_mark_completed(self):
        """Mark multiple selected in-progress cases as completed."""
        if not self.in_progress_tree or not self.in_progress_tree.selection():
            Messagebox.show_info("No Selection", "Please select in-progress cases to mark as completed.")
            return
        selected_items = self.in_progress_tree.selection() if self.in_progress_tree else []
        case_numbers = []
        case_ids = []
        for item in selected_items:
            case_id = self.in_progress_tree.item(item)['values'][0]
            if case_id:
                case_ids.append(case_id)
                case = get_in_progress_case_by_id_db(case_id)
                if case:
                    case_numbers.append(case.get('case_number', f'ID:{case_id}'))
        if not case_ids:
            Messagebox.show_error("Error", "Could not retrieve case IDs.")
            return
        cases_text = ', '.join(case_numbers)
        confirm = messagebox.askyesno(
            "Confirm Bulk Completion",
            f"Mark the following {len(case_ids)} cases as completed?\n\n{cases_text[:200]}{'...' if len(cases_text) > 200 else ''}\n\nThey will be moved to the View Data tab."
        )
        if confirm:
            completed_count = 0
            for case_id in case_ids:
                if move_case_to_completed(case_id):
                    completed_count += 1
            if completed_count > 0:
                Messagebox.show_info("Success", f"Marked {completed_count} case(s) as completed.")
                self.refresh_in_progress_view()
                self.refresh_data_view()  # Refresh main view to show completed cases
                try:
                    self.load_map_markers()
                except Exception as e:
                    logging.warning(f"Map refresh failed after bulk completion: {e}")
                self.update_status(f"Completed {completed_count} case(s).")
            else:
                Messagebox.show_error("Error", "Failed to mark cases as completed.")

    # === Phase 2: Dashboard Summary Widget ===
    
    def create_dashboard_summary(self):
        """Create a dashboard summary widget showing case statistics."""
        dashboard_frame = ttk.LabelFrame(self.in_progress_frame, text="Case Overview Dashboard", padding="10")
        dashboard_frame.grid(row=0, column=0, sticky='ew', padx=0, pady=(0,10))

        # Configure columns for the stats
        for c in range(4):
            dashboard_frame.columnconfigure(c, weight=1)

        # Total cases stats
        total_frame = ttk.Frame(dashboard_frame)
        total_frame.grid(row=0, column=0, padx=10, pady=5)

        self.total_cases_var = tk.StringVar(value="0")
        ttk.Label(total_frame, text="Total In-Progress:", font=('TkDefaultFont', 9, 'bold')).pack()
        self._total_cases_value_label = ttk.Label(total_frame, textvariable=self.total_cases_var, font=('TkDefaultFont', 14, 'bold'))
        self._total_cases_value_label.pack()

        # Priority breakdown
        priority_frame = ttk.Frame(dashboard_frame)
        priority_frame.grid(row=0, column=1, padx=10, pady=5)

        ttk.Label(priority_frame, text="By Priority:", font=('TkDefaultFont', 9, 'bold')).pack()
        self.critical_var = tk.StringVar(value="Critical: 0")
        self.high_var = tk.StringVar(value="High: 0")
        self.medium_var = tk.StringVar(value="Medium: 0")
        self.low_var = tk.StringVar(value="Low: 0")

        ttk.Label(priority_frame, textvariable=self.critical_var, foreground='red').pack(anchor='w')
        ttk.Label(priority_frame, textvariable=self.high_var, foreground='orange').pack(anchor='w')
        # Use default theme foreground for medium/low for better contrast across themes
        ttk.Label(priority_frame, textvariable=self.medium_var).pack(anchor='w')
        ttk.Label(priority_frame, textvariable=self.low_var).pack(anchor='w')

        # Due dates
        due_frame = ttk.Frame(dashboard_frame)
        due_frame.grid(row=0, column=2, padx=10, pady=5)

        ttk.Label(due_frame, text="Due Dates:", font=('TkDefaultFont', 9, 'bold')).pack()
        self.overdue_var = tk.StringVar(value="Overdue: 0")
        self.due_soon_var = tk.StringVar(value="Due Soon: 0")

        ttk.Label(due_frame, textvariable=self.overdue_var, foreground='red').pack(anchor='w')
        ttk.Label(due_frame, textvariable=self.due_soon_var, foreground='orange').pack(anchor='w')

        # Volume stats
        volume_frame = ttk.Frame(dashboard_frame)
        volume_frame.grid(row=0, column=3, padx=10, pady=5)

        ttk.Label(volume_frame, text="Total Volume:", font=('TkDefaultFont', 9, 'bold')).pack()
        self.total_volume_var = tk.StringVar(value="0 GB")
        self._total_volume_value_label = ttk.Label(volume_frame, textvariable=self.total_volume_var, font=('TkDefaultFont', 12, 'bold'))
        self._total_volume_value_label.pack()

        # Update dashboard with initial data
        self.update_dashboard_summary()
        # Apply contrast-aware colors immediately
        self.refresh_contrast_colors()
        # Initialize notification system
        self.setup_notification_system()

    def setup_notification_system(self):
        """Initialize the notification system."""
        try:
            self.check_notifications()
        except Exception:
            pass
        try:
            self.root.after(300000, self.periodic_notification_check)  # 5 minutes
        except Exception:
            pass

    def periodic_notification_check(self):
        """Periodic check for notifications."""
        try:
            self.check_notifications()
        finally:
            try:
                self.root.after(300000, self.periodic_notification_check)
            except Exception:
                pass

    def check_notifications(self):
        """Check for notification conditions and display alerts."""
        try:
            from datetime import datetime
            cases = get_all_in_progress_cases_db() or []
            notifications = []
            today = datetime.now().date()

            overdue_cases = []
            due_soon_cases = []
            for case in cases:
                due_date_str = case.get('target_due_date', '')
                case_number = case.get('case_number', f"ID-{case.get('id', '')}")
                if not due_date_str:
                    continue
                try:
                    due_date = datetime.strptime(str(due_date_str).split()[0], '%Y-%m-%d').date()
                    days_diff = (due_date - today).days
                    if days_diff < 0:
                        overdue_cases.append({
                            'case_number': case_number,
                            'days_overdue': abs(days_diff),
                            'priority': case.get('priority', 'Medium')
                        })
                    elif days_diff <= 2:
                        due_soon_cases.append({
                            'case_number': case_number,
                            'days_until_due': days_diff,
                            'priority': case.get('priority', 'Medium')
                        })
                except Exception:
                    continue

            if overdue_cases:
                critical_overdue = [c for c in overdue_cases if c.get('priority') == 'Critical']
                if critical_overdue:
                    case_list = ', '.join([c['case_number'] for c in critical_overdue[:3]])
                    if len(critical_overdue) > 3:
                        case_list += f" and {len(critical_overdue) - 3} more"
                    notifications.append({
                        'type': 'error',
                        'title': 'Critical Cases Overdue!',
                        'message': f"Critical priority cases are overdue: {case_list}"
                    })
                elif len(overdue_cases) >= 3:
                    notifications.append({
                        'type': 'warning',
                        'title': 'Multiple Cases Overdue',
                        'message': f"{len(overdue_cases)} cases are past their due dates"
                    })

            if due_soon_cases:
                critical_due_soon = [c for c in due_soon_cases if c.get('priority') == 'Critical']
                if critical_due_soon:
                    case_list = ', '.join([c['case_number'] for c in critical_due_soon[:2]])
                    notifications.append({
                        'type': 'info',
                        'title': 'Critical Cases Due Soon',
                        'message': f"Critical cases due within 2 days: {case_list}"
                    })

            self.display_notifications(notifications)
        except Exception as e:
            logging.error(f"Error checking notifications: {e}")
    
    def display_notifications(self, notifications):
        """Display notification alerts to the user."""
        if not notifications:
            return
        
        # Show the first (most important) notification
        notification = notifications[0]
        
        if notification['type'] == 'error':
            messagebox.showerror(notification['title'], notification['message'])
        elif notification['type'] == 'warning':
            messagebox.showwarning(notification['title'], notification['message'])
        elif notification['type'] == 'info':
            messagebox.showinfo(notification['title'], notification['message'])
        
        # Log notification activity
        logging.info(f"Notification displayed: {notification['title']} - {notification['message']}")

    def update_dashboard_summary(self):
        """Update the dashboard summary with current case statistics."""
        try:
            from datetime import datetime, timedelta
            
            # Get all in-progress cases
            cases = get_all_in_progress_cases_db() or []
            
            # Total cases
            total_cases = len(cases)
            self.total_cases_var.set(str(total_cases))
            
            # Priority counts
            priority_counts = {'Critical': 0, 'High': 0, 'Medium': 0, 'Low': 0}
            for case in cases:
                priority = case.get('priority', '').strip()
                if priority in priority_counts:
                    priority_counts[priority] += 1
            
            self.critical_var.set(f"Critical: {priority_counts['Critical']}")
            self.high_var.set(f"High: {priority_counts['High']}")
            self.medium_var.set(f"Medium: {priority_counts['Medium']}")
            self.low_var.set(f"Low: {priority_counts['Low']}")
            
            # Due date analysis
            today = datetime.now().date()
            overdue_count = 0
            due_soon_count = 0
            
            for case in cases:
                due_date_str = case.get('target_due_date', '')
                if due_date_str:
                    try:
                        # Parse the due date (assuming format YYYY-MM-DD)
                        due_date = datetime.strptime(due_date_str.split()[0], '%Y-%m-%d').date()
                        if due_date < today:
                            overdue_count += 1
                        elif due_date <= today + timedelta(days=7):
                            due_soon_count += 1
                    except (ValueError, AttributeError):
                        continue
            
            self.overdue_var.set(f"Overdue: {overdue_count}")
            self.due_soon_var.set(f"Due Soon: {due_soon_count}")
            
            # Total volume calculation
            total_volume = 0
            for case in cases:
                volume = case.get('volume_size_gb', 0)
                if volume:
                    try:
                        total_volume += float(volume)
                    except (ValueError, TypeError):
                        continue
            
            self.total_volume_var.set(f"{total_volume:.1f} GB")
            
        except Exception as e:
            logging.error(f"Error updating dashboard summary: {e}")
            # Set safe defaults
            self.total_cases_var.set("0")
            self.critical_var.set("Critical: 0")
            self.high_var.set("High: 0")
            self.medium_var.set("Medium: 0")
            self.low_var.set("Low: 0")
            self.overdue_var.set("Overdue: 0")
            self.due_soon_var.set("Due Soon: 0")
            self.total_volume_var.set("0 GB")

    # Activity Timeline tab removed

    def clear_entry_form(self):
        """Clears all input fields and resets editing state."""
        self.editing_case_id = None
        self.editing_in_progress_case_id = None
        if self.submit_button:
            self.submit_button.config(text="Submit Case", style="Accent.TButton")
        if hasattr(self, 'in_progress_button') and self.in_progress_button:
            self.in_progress_button.config(text="In Progress", style="Warning.TButton")
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
            # Normalize to persistent path
            path = self.logo_path.get() or LOGO_FILENAME
            # Load and scale logo for entry tab (convert for Tk)
            image = Image.open(path)
            try:
                image = image.convert('RGBA')
            except Exception:
                pass
            # Scale to reasonable height (100px) maintaining aspect ratio
            aspect_ratio = image.size[0] / image.size[1]
            new_height = 100
            new_width = int(new_height * aspect_ratio)
            image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
            self.logo_image_tk = ImageTk.PhotoImage(image)

            # Update logo in entry tab if label exists
            if self.entry_logo_label:
                self.entry_logo_label.config(image=self.logo_image_tk)

            # Create smaller version for settings preview
            preview_height = 100
            preview_width = int(preview_height * aspect_ratio)
            # Use original converted image to generate preview to avoid compounding resizes
            preview_src = Image.open(path)
            try:
                preview_src = preview_src.convert('RGBA')
            except Exception:
                pass
            preview_image = preview_src.resize((preview_width, preview_height), Image.Resampling.LANCZOS)
            self.logo_image_tk_preview = ImageTk.PhotoImage(preview_image)

            # Update preview in settings if canvas exists
            if self.logo_preview_canvas:
                self.logo_preview_canvas.delete("all")
                # Center the image in the canvas
                x = (200 - preview_width) // 2  # 200 is canvas width
                self.logo_preview_canvas.create_image(x, 0, anchor='nw', image=self.logo_image_tk_preview)

            logging.info(f"Logo loaded successfully from {path}")
        except Exception as e:
            logging.warning(f"Could not load logo image: {e}")
            # Clear existing images if load fails
            self.logo_image_tk = None
            self.logo_image_tk_preview = None
            if self.entry_logo_label:
                self.entry_logo_label.config(image='')
            if self.logo_preview_canvas:
                self.logo_preview_canvas.delete("all")
                self.logo_preview_canvas.create_text(100, 50, text="No Logo", anchor='center')

    def load_marker_icon_image(self):
        """Loads and scales the marker icon image for use in the application."""
        try:
            # Load from persistent path; convert for Tk
            path = MARKER_ICON_FILENAME
            image = Image.open(path)
            try:
                image = image.convert('RGBA')
            except Exception:
                pass
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

            logging.info(f"Marker icon loaded successfully from {path}")
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
            
            # Create the PDF document with better margins
            doc = SimpleDocTemplate(
                filename,
                pagesize=pagesize,
                rightMargin=15,
                leftMargin=15,
                topMargin=25,
                bottomMargin=25
            )

            # Prepare content elements
            elements = []

            # Add logo if available
            if hasattr(self, 'logo_image_tk') and self.logo_image_tk:
                try:
                    logo_path = self.logo_path.get()
                    logo = ReportLabImage(logo_path, width=2*inch, height=1*inch)
                    elements.append(logo)
                    elements.append(Spacer(1, 12))
                except Exception as e:
                    logging.warning(f"Could not add logo to PDF: {e}")

            # Add title using styles already defined
            title_style = styles['Title']
            elements.append(Paragraph("Case Log Report", title_style))
            elements.append(Spacer(1, 12))

            # Summary totals: Total Devices and Total Volume
            try:
                total_devices = len(cases)
                total_gb = sum(safe_float_conversion(c.get('volume_size_gb')) for c in cases)
                total_tb = total_gb / 1024.0 if total_gb > 999 else None
                elements.append(Paragraph(f"Total Devices: <b>{total_devices}</b>", styles['Normal']))
                if total_tb:
                    elements.append(Paragraph(f"Total Volume: <b>{total_tb:.2f} TB</b>", styles['Normal']))
                else:
                    elements.append(Paragraph(f"Total Volume: <b>{total_gb:.2f} GB</b>", styles['Normal']))
                elements.append(Spacer(1, 8))
            except Exception:
                pass

            # Calculate dynamic column widths based on content and page size
            page_width = pagesize[0] - 30  # Account for margins (reduced from 40)
            
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
            
            # Build the PDF
            doc.build(elements)
            
            self.update_status("PDF report generated successfully.")
            Messagebox.show_info("Success", "PDF report generated successfully.")
            
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
            
            # Export to Excel with a summary sheet first
            total_devices = len(cases)
            total_gb = sum(safe_float_conversion(c.get('volume_size_gb')) for c in cases)
            total_tb = total_gb / 1024.0 if total_gb > 999 else None
            summary_df = pd.DataFrame({
                "Total Devices": [total_devices],
                "Total Volume (GB)": [round(total_gb, 2)],
                "Total Volume (TB)": [round(total_tb, 2) if total_tb else ""]
            })

            with pd.ExcelWriter(filename) as writer:
                summary_df.to_excel(writer, sheet_name="Summary", index=False)
                df.to_excel(writer, index=False, sheet_name='Cases')
            
            self.update_status("Excel report generated successfully.")
            Messagebox.show_info("Success", "Excel report generated successfully.")
            
        except Exception as e:
            logging.error(f"Error generating Excel report: {e}")
            self.update_status("Error generating Excel report.")
            Messagebox.show_error("Error", f"Failed to generate Excel report: {e}")

    def edit_selected_case(self):
        """Loads the selected case into the entry form for editing."""
        # Get the selected item from the treeview
        if not self.tree:
            Messagebox.show_error("Error", "Case list is not available.")
            return
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
            if self.submit_button:
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
        pw = simpledialog.askstring("Password Required", 
                               "Enter password to confirm deletion:", 
                               show="*")
        if not pw:
            self.update_status("Delete cancelled (no password entered).")
            return
        if not verify_password(pw):
            Messagebox.show_error("Authentication Failed", 
                            "Incorrect password. Cases were not deleted.")
            self.update_status("Delete cancelled (incorrect password).")
            return

        # Ask for final confirmation with count of selected items
        confirm = messagebox.askyesno(
            "Confirm Deletion",
            f"Are you sure you want to delete {len(selected_items)} selected case(s)?\n"
            "This cannot be undone."
        )
        if not confirm:
            self.update_status("Delete cancelled by user.")
            return

        # Try to delete each selected case
        deleted_count = 0
        failed_count = 0

        try:
            for selected_item in selected_items:
                # Extract the database case ID from the treeview item
                if self.tree:
                    case_id = self.tree.item(selected_item)['values'][0]
                else:
                    continue
                logging.info(f"Extracted case ID from treeview: {case_id}")
                
                if delete_case_db(case_id):
                    deleted_count += 1
                else:
                    failed_count += 1
                    logging.error(f"Failed to delete case ID {case_id}")

            # Refresh the view after deletions with forced cache clear
            logging.info(f"Refreshing view after deleting {deleted_count} cases")
            # Clear any existing filter to ensure all remaining cases are visible
            if hasattr(self, 'search_var') and self.search_var:
                self.search_var.set('')
            self._view_filter_string = ''
            
            # Force complete refresh - reset lazy loading and reload from database
            self.refresh_data_view(reset_lazy=True)
            
            # Force additional refresh of treeview display
            if hasattr(self, 'tree') and self.tree:
                logging.info(f"Tree has {len(self.tree.get_children())} items after refresh")
                self.tree.update()
                self.tree.update_idletasks()
            
            # self.create_dashboard_widgets()  # Removed: dashboard no longer exists
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
            # Ensure persistent directory exists
            os.makedirs(DATA_DIR, exist_ok=True)
            # Load selected image and convert to a safe mode for PNG
            with Image.open(filename) as img:
                try:
                    # Convert palette/CMYK/LA/etc. to RGBA for consistent transparency handling
                    if img.mode not in ("RGB", "RGBA"):
                        img = img.convert("RGBA")
                except Exception:
                    pass
                # Save atomically to avoid partial writes
                tmp_path = LOGO_FILENAME + ".tmp"
                img.save(tmp_path, 'PNG')
            try:
                if os.path.exists(LOGO_FILENAME):
                    os.replace(tmp_path, LOGO_FILENAME)
                else:
                    os.rename(tmp_path, LOGO_FILENAME)
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
            # Update logo path and reload
            self.logo_path.set(LOGO_FILENAME)
            self.load_logo_image()

            logging.info(f"New logo selected and saved: {filename} -> {LOGO_FILENAME}")
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
            # Ensure persistent directory exists
            os.makedirs(DATA_DIR, exist_ok=True)
            # Load selected image and convert to safe mode
            with Image.open(filename) as img:
                try:
                    if img.mode not in ("RGB", "RGBA"):
                        img = img.convert("RGBA")
                except Exception:
                    pass
                # Save atomically
                tmp_path = MARKER_ICON_FILENAME + ".tmp"
                img.save(tmp_path, 'PNG')
            try:
                if os.path.exists(MARKER_ICON_FILENAME):
                    os.replace(tmp_path, MARKER_ICON_FILENAME)
                else:
                    os.rename(tmp_path, MARKER_ICON_FILENAME)
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
            # Reload marker icon
            self.load_marker_icon_image()

            # If map is loaded, refresh markers with new icon
            if hasattr(self, 'map_widget'):
                self.load_map_markers()

            logging.info(f"New marker icon selected and saved: {filename} -> {MARKER_ICON_FILENAME}")
            self.update_status("Marker icon updated successfully.")

        except Exception as e:
            logging.error(f"Error setting new marker icon: {e}")
            Messagebox.show_error(
                "Marker Icon Error",
                f"Could not set new marker icon:\n{str(e)}"
            )
            self.update_status("Error updating marker icon.")

    def import_cases_from_xlsx(self):
        """Imports case data from a selected XLSX file in a background thread with cancel support."""
        file_path = filedialog.askopenfilename(
            title="Select XLSX File",
            filetypes=[("Excel files", "*.xlsx")],
            initialdir=os.path.dirname(DATA_DIR)
        )
        if not file_path:
            self.update_status("XLSX import cancelled.")
            logging.info("XLSX import cancelled by user.")
            return

        if getattr(self, "_import_thread", None) and self._import_thread.is_alive():
            Messagebox.show_info("Import", "An import is already running.")
            return

        self.update_status("Importing cases from XLSX…")

        # Create a small progress dialog
        dlg = tk.Toplevel(self.root)
        dlg.title("Importing Cases…")
        dlg.geometry("360x120")
        dlg.transient(self.root)
        ttk.Label(dlg, text=os.path.basename(file_path)).pack(anchor='w', padx=10, pady=(10,4))
        pbar = ttk.Progressbar(dlg, orient="horizontal", mode="indeterminate")
        pbar.pack(fill='x', padx=10)
        pbar.start(10)
        status_lbl_var = tk.StringVar(value="Reading file and importing…")
        ttk.Label(dlg, textvariable=status_lbl_var).pack(anchor='w', padx=10, pady=(4,6))

        btns = ttk.Frame(dlg)
        btns.pack(fill='x', padx=10, pady=(0,10))
        cancel_event = threading.Event()

        result = {
            'imported': 0,
            'skipped': 0,
            'error': None,
            'cancelled': False
        }

        def on_cancel():
            cancel_event.set()
            status_lbl_var.set("Cancelling… Please wait.")

        cancel_btn = ttk.Button(btns, text="Cancel", command=on_cancel)
        cancel_btn.pack(side='right')

        # Start worker thread
        self._import_cancel_event = cancel_event
        self._import_thread = threading.Thread(
            target=self._import_cases_from_xlsx_worker,
            args=(file_path, cancel_event, result),
            daemon=True
        )
        self._import_thread.start()

        def poll():
            if self._import_thread.is_alive():
                # Update small status text occasionally
                status_lbl_var.set(f"Imported {result['imported']}… (skipped {result['skipped']})")
                self.root.after(150, poll)
                return

            # Done
            try:
                pbar.stop()
                dlg.destroy()
            except Exception:
                pass

            if result['error']:
                logging.error(f"Error importing cases from XLSX: {result['error']}")
                Messagebox.show_error("Import Error", f"Failed to import cases: {result['error']}")
                self.update_status("Import failed.")
                return

            if result['cancelled']:
                self.update_status(f"Import cancelled after {result['imported']} imported, {result['skipped']} skipped.")
                Messagebox.show_info("Import Cancelled", f"Imported: {result['imported']}\nSkipped: {result['skipped']}")
                return

            # Success path
            try:
                self.refresh_data_view()
                if hasattr(self, 'map_widget'):
                    self.load_map_markers()
                self.populate_graph_filters()
            except Exception as e:
                logging.warning(f"Post-import refresh error: {e}")

            Messagebox.show_info(
                "Import Complete",
                f"Import finished.\nImported: {result['imported']}\nSkipped: {result['skipped']}"
            )

            # Post-import optimize
            try:
                self.update_status("Optimizing database…")
                optimize_database()
                self.update_status("Database optimized.", duration=3000)
            except Exception as e:
                logging.warning(f"Optimize after import failed: {e}")

        self.root.after(150, poll)

    def _import_cases_from_xlsx_worker(self, file_path: str, cancel_event: threading.Event, result: dict):
        """Background worker to import cases from an XLSX file."""
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
            excel_columns = [str(col).strip() for col in df.columns]

            # Build mapping from Excel headers to DB keys
            excel_header_to_db_key = {}
            for col_key, config in self.tree_columns_config.items():
                if col_key in ['id', 'created_at']:
                    continue
                display_text = config.get("text", col_key)
                for excel_col in excel_columns:
                    if excel_col.strip().lower() in [display_text.strip().lower(), col_key.strip().lower()]:
                        excel_header_to_db_key[excel_col] = col_key
                        break

            # Process each row
            for idx, row in df.iterrows():
                if cancel_event.is_set():
                    result['cancelled'] = True
                    break

                case_data = {}
                for excel_col, db_key in excel_header_to_db_key.items():
                    value = row.get(excel_col)

                    # Handle dates
                    if db_key in ['start_date', 'end_date'] and pd.notnull(value):
                        try:
                            value = pd.to_datetime(value).strftime('%Y-%m-%d')
                        except Exception:
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
                    result['imported'] += 1
                else:
                    result['skipped'] += 1
                    logging.warning(f"Row {idx+1} failed to add: {case_data}")

        except Exception as e:
            result['error'] = str(e)

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
            # Get all cases (completed and in-progress)
            cases = get_all_cases_db()
            in_progress_cases = get_all_in_progress_cases_db()

            # Extract unique years from start_date
            years = set()
            for dataset in (cases, in_progress_cases):
                for case in dataset:
                    start_date = case.get('start_date', '')
                    if start_date and len(start_date) >= 4:
                        year = start_date[:4]
                        if year.isdigit():
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

    def open_backups_folder(self):
        try:
            path = os.path.abspath(BACKUP_DIR)
            if not os.path.isdir(path):
                os.makedirs(path, exist_ok=True)
            # Windows Explorer open
            os.startfile(path)
        except Exception as e:
            Messagebox.show_error("Open Folder Failed", f"Could not open backups folder.\n\n{e}")

    def backup_database_now(self):
        """Create a backup immediately and inform the user of the path."""
        try:
            path = perform_db_backup(retention_days=56, keep_last=5)
            Messagebox.show_info("Backup Complete", f"Database backup created at:\n{path}")
        except Exception as e:
            Messagebox.show_error("Backup Failed", f"Could not create backup. See log for details.\n\n{e}")

    def restore_database_from_file(self):
        """Prompt for a .db file and restore it as the active database safely."""
        try:
            file_path = filedialog.askopenfilename(
                title="Select Backup Database File",
                filetypes=[("SQLite DB", "*.db"), ("All Files", "*.*")],
                initialdir=BACKUP_DIR if os.path.isdir(BACKUP_DIR) else os.path.dirname(os.path.abspath(DB_FILENAME))
            )
            if not file_path:
                return

            # Confirm destructive action
            if not Messagebox.okcancel("Restore Database", "This will replace the current database with the selected file.\n\nA backup of the current DB will be created first. Continue?"):
                return

            # Backup current DB first
            current_backup = None
            try:
                current_backup = perform_db_backup(retention_days=56, keep_last=5)
            except Exception as e:
                logging.warning(f"Pre-restore backup failed: {e}")

            # Close any open connections we control (we open on demand in helpers), so just replace file
            src = os.path.abspath(file_path)
            dst = os.path.abspath(DB_FILENAME)
            try:
                shutil.copy2(src, dst)
            except PermissionError as e:
                Messagebox.show_error("Restore Failed", f"Permission denied replacing DB. Ensure the app isn't locking the file.\n\n{e}")
                return
            except Exception as e:
                Messagebox.show_error("Restore Failed", f"Unexpected error during restore.\n\n{e}")
                return

            # Notify and restart app recommendation
            extra = f"\n\nA pre-restore backup was saved to:\n{current_backup}" if current_backup else ""
            Messagebox.show_info("Restore Complete", f"Database has been restored from:\n{src}{extra}\n\nPlease restart the application to ensure all data is reloaded.")
        except Exception as e:
            logging.error(f"Restore failed: {e}")
            Messagebox.show_error("Restore Failed", f"Restore encountered an error. See log for details.\n\n{e}")

if __name__ == "__main__":
    # Initialize the database
    init_db()

    # Create the main window with the specified theme
    root = tb.Window(themename="flatly")
    _set_window_icon(root)
    try:
        root.title(APP_NAME)
    except Exception:
        pass

    # Sleek centered overlay loader with logo and status text
    try:
        # Hide root so only one window (overlay) is visible during startup
        try:
            root.withdraw()
        except Exception:
            pass
        overlay = tk.Toplevel(root)
        overlay.overrideredirect(True)
        overlay.attributes('-topmost', True)
        # Semi-transparent overall window
        try:
            overlay.wm_attributes('-alpha', 0.88)
        except Exception:
            try:
                overlay.attributes('-alpha', 0.88)
            except Exception:
                pass
        # Center overlay relative to the screen
        sw = root.winfo_screenwidth()
        sh = root.winfo_screenheight()
        ow, oh = 420, 220
        ox = int((sw - ow) / 2)
        oy = int((sh - oh) / 3)
        overlay.geometry(f"{ow}x{oh}+{ox}+{oy}")

        # Use Tk widgets with explicit dark background to avoid white flash
        overlay.configure(bg='#222222')
        container = tk.Frame(overlay, bg='#222222')
        container.pack(fill='both', expand=True, padx=20, pady=20)

        # Logo (if available)
        logo_label = tk.Label(container, bg='#222222', fg='#DDDDDD')
        logo_label.pack(pady=(8, 6))
        logo_img_ref = None
        if os.path.exists(LOGO_FILENAME):
            try:
                _img = Image.open(LOGO_FILENAME)
                _img = _img.convert('RGBA')
                # Robust resampling: try Resampling.LANCZOS, then LANCZOS, then BICUBIC
                try:
                    resample = getattr(getattr(Image, 'Resampling', Image), 'LANCZOS', None)
                    if resample is None:
                        resample = getattr(Image, 'LANCZOS', None)
                    if resample is None:
                        resample = getattr(Image, 'BICUBIC', 3)
                except Exception:
                    resample = 3  # BICUBIC
                _img.thumbnail((96, 96), resample)
                logo_img_ref = ImageTk.PhotoImage(_img)
                logo_label.configure(image=logo_img_ref)
            except Exception:
                logo_label.configure(text="Loading…")
        else:
            logo_label.configure(text="Loading…")

        status_var = tk.StringVar(value="Initializing…")
        status_label = tk.Label(container, textvariable=status_var, font=('TkDefaultFont', 11), bg='#222222', fg='#FFFFFF')
        status_label.pack(pady=(0, 8))

        try:
            pbar = tb.Progressbar(container, mode='indeterminate', length=200)
        except Exception:
            pbar = ttk.Progressbar(container, mode='indeterminate', length=200)
        pbar.pack(pady=(0, 8))
        try:
            pbar.start(12)
        except Exception:
            pass

        # Helper to update overlay status safely
        def set_loader_status(msg: str):
            try:
                status_var.set(msg)
                overlay.update_idletasks()
            except Exception:
                pass
    except Exception:
        overlay = None
        pbar = None
        logo_img_ref = None

    def _start_app():
        try:
            if overlay:
                set_loader_status("Starting application…")
            app = CaseLogApp(root)
            # Wire the close handler
            root.protocol("WM_DELETE_WINDOW", app.on_closing)
        finally:
            # Remove overlay once initialized
            try:
                if pbar:
                    pbar.stop()
            except Exception:
                pass
            try:
                if overlay and overlay.winfo_exists():
                    overlay.destroy()
            except Exception:
                pass
            # Show the main window now that the app is ready
            try:
                root.deiconify()
            except Exception:
                pass

    # Let Tk loop create the app after a short delay so splash paints
    root.after(50, _start_app)

    # Start the main event loop
    root.mainloop()