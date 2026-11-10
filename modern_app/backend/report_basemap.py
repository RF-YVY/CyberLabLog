"""Embed a public USGS background at generation time; no case details leave the app."""
import base64
import hashlib
import json
import math
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from coordinates import coordinate_pair
from paths import data_dir

SERVICE = "https://basemap.nationalmap.gov/arcgis/rest/services/USGSTopo/MapServer/export"
ATTRIBUTION = "USGS The National Map; U.S. Census Bureau TIGER/Line; other USGS basemap contributors"


def report_bounds(points):
    pairs = [pair for p in points if (pair := coordinate_pair(p.get("latitude"), p.get("longitude"))) is not None]
    if not pairs:
        return [-91.8, 30.0, -88.0, 35.1]
    latitudes, longitudes = zip(*pairs)
    dx = max(max(longitudes) - min(longitudes), 0.7) * 0.25
    dy = max(max(latitudes) - min(latitudes), 0.5) * 0.25
    return [max(-180, min(longitudes) - dx), max(-85, min(latitudes) - dy), min(180, max(longitudes) + dx), min(85, max(latitudes) + dy)]


def valid_background(value):
    if not isinstance(value, dict):
        return {}
    image = value.get("image", "")
    bounds = value.get("bounds")
    if not isinstance(image, str) or not image.startswith("data:image/jpeg;base64,") or len(image) > 8 * 1024 * 1024:
        return {}
    try:
        if not base64.b64decode(image.split(",", 1)[1], validate=True).startswith(b"\xff\xd8\xff"):
            return {}
        south, west, north, east = [float(v) for pair in bounds for v in pair]
        if not all(math.isfinite(v) for v in [south, west, north, east]) or not (-90 <= south < north <= 90 and -180 <= west < east <= 180):
            return {}
    except (ValueError, TypeError):
        return {}
    return {"image": image, "bounds": [[south, west], [north, east]], "attribution": ATTRIBUTION}


def _read_url(url, limit):
    request = Request(url, headers={"User-Agent": "CyberLab-Case-Tracker/3 report export"})
    with urlopen(request, timeout=20) as response:
        data = response.read(limit + 1)
    if len(data) > limit:
        raise ValueError("Map background response is too large")
    return data


def fetch_background(points):
    bbox = [round(value, 4) for value in report_bounds(points)]
    query = urlencode({"f": "json", "bbox": ",".join(map(str, bbox)), "bboxSR": 4326, "imageSR": 3857, "size": "1800,1200", "format": "jpg", "dpi": 150})
    cache = data_dir() / "map_backgrounds" / (hashlib.sha256(query.encode()).hexdigest() + ".json")
    try:
        if cache.exists():
            cached = valid_background(json.loads(cache.read_text(encoding="utf-8")))
            if cached:
                return cached
        metadata = json.loads(_read_url(SERVICE + "?" + query, 65536))
        image_url = metadata["href"]
        parsed = urlparse(image_url)
        if parsed.scheme != "https" or parsed.netloc != "basemap.nationalmap.gov" or not parsed.path.startswith("/arcgis/rest/directories/"):
            raise ValueError("Unexpected map image URL")
        image = _read_url(image_url, 6 * 1024 * 1024)
        extent = metadata["extent"]
        def lon(x): return math.degrees(float(x) / 6378137)
        def lat(y): return math.degrees(math.atan(math.sinh(float(y) / 6378137)))
        result = valid_background({"image": "data:image/jpeg;base64," + base64.b64encode(image).decode("ascii"), "bounds": [[lat(extent["ymin"]), lon(extent["xmin"])], [lat(extent["ymax"]), lon(extent["xmax"])]]})
        if result:
            try:
                cache.parent.mkdir(parents=True, exist_ok=True)
                cache.write_text(json.dumps(result), encoding="utf-8")
            except OSError:
                pass  # A read-only cache must not discard a successful map.
        return result
    except Exception:
        return {}  # County lines, names, and markers remain usable offline.


@lru_cache(maxsize=1)
def all_counties():
    return json.loads((Path(__file__).parent / "map_assets" / "us_counties_2025_5m.geojson").read_text(encoding="utf-8"))["features"]


def county_features(points, background=None):
    if background:
        (south, west), (north, east) = background["bounds"]
    else:
        west, south, east, north = report_bounds(points)
    features = [f for f in all_counties() if f["bbox"][0] <= east and f["bbox"][2] >= west and f["bbox"][1] <= north and f["bbox"][3] >= south]
    return {"type": "FeatureCollection", "features": features}
