# Geospatial

Geospatial data is downloaded from [GADM](https://gadm.org/). Data is
downloaded in GeoJSON format ([RFC
7946](https://datatracker.ietf.org/doc/html/rfc7946)) and the
[Shapefile](https://en.wikipedia.org/wiki/Shapefile) format.

Shapefiles provided by GADM can be converted into [GeoDataFrame] objects from the GeoPandas library:

```python
import geopandas as gpd
df = gpd.read_file("gadm41_{iso3}_{admin_level}.shp")
```

where `iso3` is the [ISO 3166-2 alpha-3
code](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) codes, and
`admin_level` ranges from 0 to 3 inclusive. Admin level 0 is the country level,
admin level 1 state or province, admin level 2 district and admin level 3
further subdivisions such as city level.

GADM shapefiles usually have the following
[columns](https://gadm.org/metadata.html). These are the column names once read
in using `gpd.read_file()`:

- **`GID_0`**: The ISO3 code for the country
- **`COUNTRY`**: Common name of the country in English
- **`GID_i`**: Unique GADM ID for admin level `i` where `i` is one of 1,2,3.
  GIDs are usually nested with a period (`.`) with lower level GIDs sharing a
  prefix with admin levels higher. There is an overall version prefix starting
  from `_1`
- **`NAME_i`**: Name of the geographical area in Latin script
- **`NL_NAME_i`**: Official name in non-Latin script
- **`VARNAME_i`**: Variant or alternate name. Can have multiple names,
  separated by `|`.
- **`TYPE_i`**: Administrative type in local language
- **`ENGTYPE_i`**: Administrative type in English
- **`CC_i`**: Unique code used by the country, if present, `NA` otherwise
- **`HASC_i`**: Unique [HASC](https://statoids.com/ihasc.html) ID.
- **`geometry`**: Geometry column containing the set of polygons representing
  the boundary of the administrative area

When reading in a shapefile at admin level `i`, all `GID_k` (and accompanying
`NAME_k`, `NL_NAME_k`, but not `VARNAME_k`) are present for `k < i`.
