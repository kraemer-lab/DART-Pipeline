# Using custom shapefiles

By default, DART uses [GADM](https://gadm.org) shapefiles as the geometry input
to the pipeline. While this is a good default, you may want to supply your own
shapefiles, in case the country has updated administrative boundaries since the
last GADM update, or you only want to run DART on a sub-region of a country,
such as a single province.

The custom shapefile configuration is located at
`~/.local/share/dart-pipeline/regions.toml` for Linux and macOS or at
`C:\Users\{username}\AppData\Local\dart-pipeline\regions.toml` on Windows. The
TOML file has keys, each of which is the name of a region (usually uppercase
by convention, no hyphens allowed), with key value pairs denoting the configuration.

The following keys should be present for each region defined in the file:

bbox
: Bounding box of the region, specified as `[minx, miny, maxx, maxy]` where `x` is longitude and `y` is latitude

url
: URL where region data was downloaded from

iso3
: If specified refers to the ISO3 code this region refers to, or is a sub-region of.
  For example if the region key is `CA` representing California, iso3 should be set to USA.
  Optional, leave blank if region covers multiple countries.

tz
: Time zone of the region, specified as +HH:MM or -HH:MM as shift from UTC

admin_files
: Mapping from administrative levels (1, 2, 3) to shapefiles (`.shp` extension)

pk
: Mapping from administrative levels to a primary key column that uniquely identifies an administrative unit within the shapefile.
  If all administrative levels share the same primary key column, can be specified as a string.

All values, unless numbers or booleans must be quoted in TOML. An example TOML file is shown below:

```toml
[VNM]
bbox = [102,7,118,24]
url = "https://www.geoboundaries.org/api/current/gbOpen/VNM/"
iso3 = "VNM"
tz = "+07:00"
admin_files = {2 = "data/VNM/geoboundaries/geoBoundaries-VNM-ADM2.shp"}
pk = {2 = "shapeID"}
```
