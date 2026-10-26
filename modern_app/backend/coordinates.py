"""Shared interpretation of stored case-location coordinate pairs."""
import math


def coordinate_pair(latitude, longitude):
    try:
        if latitude in (None, "") or longitude in (None, ""):
            return None
        lat, lon = float(latitude), float(longitude)
        if not math.isfinite(lat) or not math.isfinite(lon):
            return None
        if not -90 <= lat <= 90 or not -180 <= lon <= 180:
            return None
        # Legacy/imported named locations use Null Island as a missing-value sentinel.
        if lat == 0 and lon == 0:
            return None
        return lat, lon
    except (ValueError, TypeError, OverflowError):
        return None
