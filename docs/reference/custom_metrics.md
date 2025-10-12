# Custom metrics

Custom metrics, such as for land cover type or mosquito incidence can be added
to the pipeline. For a fully custom configuration, it is recommended to work
directly with the underlying [geoglue](https://geoglue.readthedocs.io) library,
for which we provide an example of [performing zonal
aggregation](https://geoglue.readthedocs.io/en/latest/tutorial/zonal_stats.html)
in its online documentation.

Metrics can also be integrated into the DART-Pipeline command line interface,
such that they can be processed using `uv run dart-pipeline` as described in
the <project:./metrics.md> page. This can be done by cloning the [DART-Pipeline
repository](https://github.com/kraemer-lab/DART-Pipeline) and placing a new
file under `src/dart_pipeline/metrics` that follows a certain format. Metric
processing code in the DART-Pipeline is divided into three parts: declaring the metric metadata, fetching and
processing.

## Declaring metadata

Each Python code file under `src/dart_pipeline/metrics/` represents a single data source. If your code is complex, it is recommended to create a snuffled under `src/dart_pipeline/metrics`.

Metadata is declared for a source (which can comprise multiple metrics) by calling the `register_metrics()` function defined in the `dart_pipeline.metrics` submodule.

Each metric has associated metadata, starting with the name, which should have
lowercase characters, with underscore (`_`) separating words. The other
metadata are as follows. [CF Convention](https://cfconventions.org/)-compliant
attributes that are saved as part of the netCDF file are indicated as [CF]

url
: URL of data source

long_name
: Description of the metric [CF]

units
: Unit of the metric [CF], should correspond to [UDUNITS](https://docs.unidata.ucar.edu/udunits/current/#Database)
  which is recommended by the CF Conventions

valid_min
: Minimum value of the metric, values below this will be considered as NA [CF]

valid_max
: Maximum value of the metric, values above this will be considered as NA [CF]

license
: License of the underlying data, should follow the list of [SPDX licenses](https://spdx.org/licenses/)

citation
: Free text describing required citation information for using the underlying data

You can see an [example
registration](https://github.com/kraemer-lab/DART-Pipeline/blob/main/src/dart_pipeline/metrics/worldpop.py#L30-L54)
for the `worldpop.pop_count` data source:

```python
register_metrics(
    "worldpop",
    description="WorldPop population data",
    metrics={
        "pop_count": {
            "url": "https://hub.worldpop.org/geodata/listing?id=75",
            "long_name": "WorldPop population count",
            "units": "unitless",
            "license": "CC-BY-4.0",
            # We produce outputs at minimum admin1 resolution, unlikely
            # that any administrative area will have population greater than this
            "valid_min": 0,
            "citation": """
             WorldPop (www.worldpop.org - School of Geography and Environmental
             Science, University of Southampton; Department of Geography and
             Geosciences, University of Louisville; Departement de Geographie,
             Universite de Namur) and Center for International Earth Science
             Information Network (CIESIN), Columbia University (2018). Global
             High Resolution Population Denominators Project - Funded by The
             Bill and Melinda Gates Foundation (OPP1134076).
             https://dx.doi.org/10.5258/SOTON/WP00671
             """,
        },
    },
)
```

## Fetching data

To fetch data, define a function and decorate it with
`@register_fetch("source.metric_name")`. The first parameter of the function
**must** be of a [geoglue.region
type](https://geoglue.readthedocs.io/en/latest/reference/geoglue.html#module-geoglue.region)
(BaseRegion, ZonedBaseRegion, BaseCountry, Region, AdministrativeLevel, or
CountryAdministrativeLevel). If the function takes a temporal scope (year,
year-month, date or year range), the second parameter **must** be named `date`
to work with the DART CLI. The rest of the parameter signature can be custom
parameters, which can be passed through the CLI as `param=value`. For example, the fetcher for `worldpop.pop_count` is defined as follows


```python
@register_fetch("worldpop.pop_count")
def worldpop_pop_count_fetch(region: BaseCountry, date: str) -> Literal[False]:
```

The return type can be (for type definitions, see
[src/dart_pipeline/types.py](https://github.com/kraemer-lab/DART-Pipeline/blob/main/src/dart_pipeline/types.py))

- `Literal[False]`: The function directly downloads data. This skips any
  automated processing that DART-Pipeline may perform after fetching data
- `DataFile`: The function downloads into a temporary buffer, which is then
  processed by DART to save the file to disk
- `URLCollection`: A type that comprises a base URL and a list of files, with
  optional attributes indicating whether ZIP files should be extracted. If the
  fetcher function returns this type, then DART-Pipeline automatically fetches
  the data and extracts it to a standardised location, saving you the effort of
  writing boilerplate downloading code.


## Processing data

Similarly to fetching data, the first parameter of the processing function
**must** be a geoglue.region type, and the second parameter should be a date,
if the processing requires a temporal scope. The return type **must** be one of
these types:

- `pd.DataFrame`: The function can return a pandas DataFrame. The DataFrame
  must contain a `DART_REGION` column containing the region name, and a
  dataframe attribute `admin` representing the administrative level. This is
  used by DART to save the file to the appropriate output location with a
  standardised file name.
- `xr.DataArray` | `xr.Dataset`: The function returns an annotated xarray
  Dataset or DataArray. We recommend using the `zonal_stats_xarray()` function
  in `dart_pipeline.util` which can perform zonal statistics for a given source
  raster and geoglue.region type, while adding appropriate metadata. If writing
  your own function for zonal aggregation, it is recommended to add
  [CF-compliant](https://cfconventions.org) attributes and the `DART_region`
  attribute that stores a string representation of the geoglue.region type
  passed to the processing function.

The function should be decorated using the `register_process()` decorator, as
seen in this snippet for the `worldpop.pop_count` metric:

```python
@register_process("worldpop.pop_count")
def worldpop_pop_count_process(region: CountryAdministrativeLevel, date: str) -> xr.DataArray:
```
