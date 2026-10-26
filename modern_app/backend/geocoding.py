from __future__ import annotations

import json
import logging
import sqlite3
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any

from database import connect, ensure_schema
from coordinates import coordinate_pair


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "CyberLab-Case-Tracker/3.0 (case-location-geocoder)"


def location_key(payload: dict[str, Any]) -> str:
    location = str(payload.get("location_name") or "").strip()
    city = str(payload.get("city_of_offense") or "").strip()
    state = str(payload.get("state_of_offense") or "").strip()
    return "|".join(part for part in ([location, city, state] if location else [city, state]))


def location_query(payload: dict[str, Any]) -> str:
    parts = [
        str(payload.get("location_name") or "").strip(),
        str(payload.get("city_of_offense") or "").strip(),
        str(payload.get("state_of_offense") or "").strip(),
        "USA",
    ]
    seen: set[str] = set()
    unique = []
    for part in parts:
        marker = part.casefold()
        if part and marker not in seen:
            seen.add(marker)
            unique.append(part)
    return ", ".join(unique)


def location_queries(payload: dict[str, Any]) -> list[str]:
    """Return increasingly broad queries without ever discarding a supplied landmark."""
    location = str(payload.get("location_name") or "").strip()
    state = str(payload.get("state_of_offense") or "").strip()
    candidates = [location_query(payload)]
    if location:
        candidates.extend(
            query for query in (
                ", ".join(part for part in (location, state, "USA") if part),
                ", ".join(part for part in (location, "USA") if part),
            )
            if query
        )
    unique: list[str] = []
    seen: set[str] = set()
    for query in candidates:
        marker = query.casefold()
        if query and marker not in seen:
            seen.add(marker)
            unique.append(query)
    return unique


def cached_coordinates(key: str) -> tuple[float, float] | None:
    if not key:
        return None
    ensure_schema()
    with connect() as conn:
        row = conn.execute(
            "SELECT latitude, longitude FROM geocache WHERE location_key = ?",
            (key,),
        ).fetchone()
    return coordinate_pair(row["latitude"], row["longitude"]) if row else None


def cache_coordinates(key: str, latitude: float, longitude: float) -> None:
    pair = coordinate_pair(latitude, longitude)
    if pair is None:
        raise ValueError("Cannot cache an unresolved coordinate pair")
    latitude, longitude = pair
    ensure_schema()
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO geocache (location_key, latitude, longitude, last_accessed)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(location_key) DO UPDATE SET
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                last_accessed = excluded.last_accessed
            """,
            (key, latitude, longitude, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()


def geocode_payload(payload: dict[str, Any], timeout: float = 5.0) -> dict[str, Any] | None:
    """Resolve a case location, preferring manual coordinates and the local cache."""
    latitude = payload.get("latitude")
    longitude = payload.get("longitude")
    key = location_key(payload)
    pair = coordinate_pair(latitude, longitude)
    if pair is not None:
        result = {"latitude": pair[0], "longitude": pair[1], "source": "manual", "location_key": key}
        if key:
            cache_coordinates(key, result["latitude"], result["longitude"])
        return result
    if not key:
        return None
    cached = cached_coordinates(key)
    if cached:
        return {"latitude": cached[0], "longitude": cached[1], "source": "cache", "location_key": key}

    queries = location_queries(payload)
    if not queries:
        return None
    for index, query in enumerate(queries):
        if index:
            time.sleep(1.05)
        params = urllib.parse.urlencode({"q": query, "format": "jsonv2", "limit": 1, "countrycodes": "us"})
        request = urllib.request.Request(f"{NOMINATIM_URL}?{params}", headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                matches = json.loads(response.read().decode("utf-8"))
            if not matches:
                continue
            latitude = float(matches[0]["lat"])
            longitude = float(matches[0]["lon"])
            if coordinate_pair(latitude, longitude) is None:
                continue
            cache_coordinates(key, latitude, longitude)
            return {
                "latitude": latitude,
                "longitude": longitude,
                "display_name": matches[0].get("display_name", query),
                "source": "nominatim",
                "location_key": key,
            }
        except (OSError, ValueError, KeyError, json.JSONDecodeError, sqlite3.DatabaseError) as exc:
            logging.warning("Could not geocode %r: %s", query, exc)
            return None
    return None


def ensure_location_cached(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Best-effort geocoding used after case writes; case saving must not depend on network access."""
    try:
        return geocode_payload(payload)
    except Exception as exc:
        logging.warning("Location caching failed after case save: %s", exc)
        return None


def geocode_missing_case_locations(timeout: float = 5.0) -> dict[str, Any]:
    """Geocode each unique existing case location that has no coordinates or cache entry."""
    ensure_schema()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, 'case_log' AS source_table, location_name, city_of_offense, state_of_offense, latitude, longitude FROM case_log
            UNION ALL
            SELECT id, 'in_progress_cases' AS source_table, location_name, city_of_offense, state_of_offense, latitude, longitude FROM in_progress_cases
            """
        ).fetchall()

    unique: dict[str, dict[str, Any]] = {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        payload = dict(row)
        key = location_key(payload)
        if key:
            grouped.setdefault(key, []).append(payload)
            if key not in unique or (coordinate_pair(unique[key].get("latitude"), unique[key].get("longitude")) is None and coordinate_pair(payload.get("latitude"), payload.get("longitude")) is not None):
                unique[key] = payload

    repaired_cases = 0

    def repair_records(key, pair):
        nonlocal repaired_cases
        with connect() as conn:
            for row in grouped[key]:
                if coordinate_pair(row["latitude"], row["longitude"]) is not None:
                    continue
                # Match the snapshot values so a concurrent manual edit wins.
                repaired_cases += conn.execute(
                    f"UPDATE {row['source_table']} SET latitude = ?, longitude = ? WHERE id = ? AND latitude IS ? AND longitude IS ?",
                    (*pair, row["id"], row["latitude"], row["longitude"]),
                ).rowcount
            conn.commit()

    pending: list[dict[str, Any]] = []
    already_mapped = 0
    for key, payload in unique.items():
        pair = cached_coordinates(key) or coordinate_pair(payload.get("latitude"), payload.get("longitude"))
        if pair is not None:
            cache_coordinates(key, *pair)
            repair_records(key, pair)
            already_mapped += 1
        else:
            pending.append(payload)

    geocoded = 0
    unresolved: list[str] = []
    for index, payload in enumerate(pending):
        if index:
            time.sleep(1.05)
        result = geocode_payload(payload, timeout=timeout)
        pair = coordinate_pair(result.get("latitude"), result.get("longitude")) if result else None
        if pair is not None:
            repair_records(location_key(payload), pair)
            geocoded += 1
        else:
            unresolved.append(location_query(payload))

    return {
        "locations_checked": len(unique),
        "already_mapped": already_mapped,
        "missing_checked": len(pending),
        "geocoded": geocoded,
        "repaired_cases": repaired_cases,
        "unresolved": unresolved,
    }
