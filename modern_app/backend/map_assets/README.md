# Standalone map assets

Exported HTML embeds these assets so it works from `file://` without internet or third-party tile requests. Country and U.S. state shapes provide an overview, not street-level or survey boundaries. Street tiles are enabled only from HTTP(S), with a normal browser referrer and visible attribution.

- Leaflet 1.9.4: copied from the pinned frontend npm dependency; BSD 2-Clause license in `LEAFLET-LICENSE.txt`. CSS images are embedded as data URLs and the source-map reference is removed.
- Natural Earth 1:110m countries and states/provinces: public domain, https://www.naturalearthdata.com/about/terms-of-use/ . Feature properties are reduced to display names; geometry is unchanged.

Source files retrieved October 26, 2026 from the Natural Earth maintainer repository:

https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_110m_admin_0_countries.geojson

Original SHA-256: `6866c877d39cba9c357620878839b336d569f8c662d3cfab4cb1dbe2d39c977f`

https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_110m_admin_1_states_provinces.geojson

Original SHA-256: `0067dec6a2c4f9c7a644bf7a9a46163d8d881595953326d0c67d89934190aefe`
