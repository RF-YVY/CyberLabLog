# Standalone map assets

Exported HTML embeds these assets so it works from `file://` without internet or third-party tile requests. The default custom county map uses pale state-based land fills, blue water, thin county lines, muted county names, and selected city labels. No external service is required for either generation or viewing. Label spacing considers both other labels and case markers. No OSM tiles are requested.

When `include_usgs_background` is selected in report output options, generation also embeds a USGS topographic JPEG. It uses the public National Map export service, requesting only the geographic extent, size, and projection; case data and markers are never sent. Images are cached under `app_data/map_backgrounds` and reused for the same extent. USGS is an optional layer, never the initially selected background. County land fills become transparent when this background is selected, and return when switching back to the custom map. County boundaries and names remain available if the optional background request fails.

- Leaflet 1.9.4: copied from the pinned frontend npm dependency; BSD 2-Clause license in `LEAFLET-LICENSE.txt`. CSS images are embedded as data URLs and the source-map reference is removed.
- Natural Earth 1:110m countries and states/provinces: public domain, https://www.naturalearthdata.com/about/terms-of-use/ . Feature properties are reduced to display names; geometry is unchanged.
- U.S. Census Bureau 2025 cartographic county boundaries, 1:5,000,000: `us_counties_2025_5m.geojson`. Retrieved November 10, 2026 from https://www2.census.gov/geo/tiger/GENZ2025/shp/cb_2025_us_county_5m.zip . Converted from shapefile with pyshp and Shapely; simplified by 0.003 degrees with topology preservation and rounded to five decimal places. Includes 3,235 county-equivalent features, GEOID, full geographic name, bounding box, and an interior label point. This is a generalized reference map, not survey geometry. Conversion libraries are development tools, not runtime dependencies. Only counties intersecting a report's extent are embedded. County labels abbreviate suffixes and avoid collisions as the map is zoomed and panned; hover shows the full geographic name.
- USGS background service: https://basemap.nationalmap.gov/arcgis/rest/services/USGSTopo/MapServer . Embedded backgrounds retain their geographic extent and attribution. Zooming magnifies the saved image; it does not download finer-resolution tiles. Report viewing performs no background-service calls for reports containing an embedded image.

Source files retrieved October 26, 2026 from the Natural Earth maintainer repository:

https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_110m_admin_0_countries.geojson

Original SHA-256: `6866c877d39cba9c357620878839b336d569f8c662d3cfab4cb1dbe2d39c977f`

https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_110m_admin_1_states_provinces.geojson

Original SHA-256: `0067dec6a2c4f9c7a644bf7a9a46163d8d881595953326d0c67d89934190aefe`
