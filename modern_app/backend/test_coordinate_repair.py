import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import database
import geocoding
import native_exports
from coordinates import coordinate_pair


class CoordinateRepairTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(database, "active_db_path", lambda: Path(self.temp.name) / "test.db")
        self.db_patch.start()
        database.ensure_schema()

    def tearDown(self):
        self.db_patch.stop()
        self.temp.cleanup()

    def case(self, number, lat=0, lon=0, active=False):
        return database.create_case({"case_number": number, "city_of_offense": "Example City", "state_of_offense": "MS", "latitude": lat, "longitude": lon}, in_progress=active)

    def poisoned_cache(self):
        with database.connect() as conn:
            conn.execute("INSERT INTO geocache (location_key, latitude, longitude) VALUES ('Example City|MS',0,0)")
            conn.commit()

    def test_zero_pair_and_invalid_pairs_are_missing_but_single_zero_is_valid(self):
        for pair in [(0, 0), ("0.00", "0.00"), (None, 2), (91, 20), (20, float('inf')), ("bad", 10)]:
            self.assertIsNone(coordinate_pair(*pair))
        self.assertEqual(coordinate_pair(0, 30), (0, 30))
        self.assertEqual(coordinate_pair(30, 0), (30, 0))

    def test_refresh_retries_poisoned_cache_and_repairs_both_case_tables(self):
        completed = self.case("TEST-1")
        active = self.case("TEST-2", active=True)
        self.poisoned_cache()
        response = MagicMock()
        response.__enter__.return_value = io.BytesIO(json.dumps([{"lat": "34.5", "lon": "-89.5"}]).encode())
        with patch.object(geocoding.urllib.request, "urlopen", return_value=response) as lookup:
            result = geocoding.geocode_missing_case_locations()
        self.assertEqual(lookup.call_count, 1)
        self.assertEqual(result["geocoded"], 1)
        self.assertEqual(result["repaired_cases"], 2)
        self.assertEqual(database.get_case(completed["id"])["latitude"], 34.5)
        self.assertEqual(database.get_case(active["id"], True)["longitude"], -89.5)
        self.assertEqual(geocoding.cached_coordinates("Example City|MS"), (34.5, -89.5))
        with patch.object(geocoding.urllib.request, "urlopen") as repeated:
            self.assertEqual(geocoding.geocode_missing_case_locations()["repaired_cases"], 0)
            repeated.assert_not_called()

    def test_valid_cache_overrides_zero_cases_for_live_and_exported_maps(self):
        row = self.case("TEST-1")
        geocoding.cache_coordinates("Example City|MS", 34.5, -89.5)
        self.assertEqual(database.get_map_markers()[0]["latitude"], 34.5)
        self.assertEqual(native_exports._map_points([row])[0]["longitude"], -89.5)
        with patch.object(geocoding.urllib.request, "urlopen") as lookup:
            result = geocoding.geocode_missing_case_locations()
        lookup.assert_not_called()
        self.assertEqual(result["repaired_cases"], 1)

    def test_valid_record_repairs_other_records_without_overwriting_precision(self):
        invalid = self.case("TEST-1")
        precise = self.case("TEST-2", 34.56789, -89.56789)
        other = self.case("TEST-3", 34.67891, -89.67891)
        self.poisoned_cache()
        with patch.object(geocoding.urllib.request, "urlopen") as lookup:
            result = geocoding.geocode_missing_case_locations()
        lookup.assert_not_called()
        self.assertEqual(result["repaired_cases"], 1)
        self.assertEqual(database.get_case(invalid["id"])["latitude"], precise["latitude"])
        self.assertEqual(database.get_case(other["id"])["latitude"], other["latitude"])

    def test_failed_lookup_stays_unresolved_instead_of_plotting_null_island(self):
        self.case("TEST-1")
        self.poisoned_cache()
        with patch.object(geocoding.urllib.request, "urlopen", side_effect=OSError("offline")):
            result = geocoding.geocode_missing_case_locations()
        self.assertEqual(result["repaired_cases"], 0)
        self.assertEqual(len(result["unresolved"]), 1)
        self.assertIsNone(database.get_map_markers()[0]["latitude"])

    def test_saving_zero_pair_cannot_poison_a_valid_cache(self):
        geocoding.cache_coordinates("Example City|MS", 34.5, -89.5)
        result = geocoding.geocode_payload({"city_of_offense": "Example City", "state_of_offense": "MS", "latitude": 0, "longitude": 0})
        self.assertEqual(result["source"], "cache")
        self.assertEqual(geocoding.cached_coordinates("Example City|MS"), (34.5, -89.5))

    def test_concurrent_manual_correction_is_not_overwritten(self):
        row = self.case("TEST-1")
        def resolve(payload, timeout=5):
            with database.connect() as conn:
                conn.execute("UPDATE case_log SET latitude=35.123, longitude=-90.123 WHERE id=?", (row["id"],))
                conn.commit()
            return {"latitude": 34.5, "longitude": -89.5}
        with patch.object(geocoding, "geocode_payload", side_effect=resolve):
            result = geocoding.geocode_missing_case_locations()
        self.assertEqual(result["repaired_cases"], 0)
        self.assertEqual(database.get_case(row["id"])["latitude"], 35.123)
