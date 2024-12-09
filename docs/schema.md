# Schema

All DART pipeline processors output data in an uniform schema, described below.
Data is usually made available as text CSV files or in the parquet format.

- **`iso3`**: The [ISO 3166-1
  alpha-3](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) country code for
  the measure. The [pycountry](https://pypi.org/p/pycountry) package is used to
  map ISO codes to country names.
- **`admin_level_0`**: The common name of the country referred to in `iso3`
- **`admin_level_1`**: Admin level 1 of the country (state or province)
- **`admin_level_2`**: Admin level 2 of the country (district)
- **`admin_level_3`**: Admin level 3 of the country (city)
- **`year`**: Year of measurement
- **`month`**: Month of measurement (1-12)
- **`day`**: Day of measurement (1-31)
- **`week`**: Week of measurement (1-52)
- **`metric`**: Name of the metric, possible values can be seen in the
  [metrics](metrics/index.md) page
- **`value`**: Value of the metric
- **`unit`**: Unit of the metric, specified as an [UCUM
  code](https://github.com/ucum-org/ucum/blob/main/common-units/TableOfExampleUcumCodesForElectronicMessagingwithPreface.pdf)
- **`resolution`**: Geographical resolution of the measure, one of `world`,
  `admin0`, `admin1`, `admin2` or `admin3`.
- **`creation_date`**: Date measure was recorded in DART database, in YYYY-MM-DD
  format.

The schema is available as a [JSON Schema for
validation](https://github.com/kraemer-lab/DART-Pipeline/blob/main/dart-pipeline.schema.json).
JSON Schema can be used to validate data files, using packages such as
[{jsonvalidate}](https://cran.r-project.org/web/packages/jsonvalidate/vignettes/jsonvalidate.html)
in R or [fastjsonschema](https://horejsek.github.io/python-fastjsonschema/) in
Python.
