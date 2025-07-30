# Overview

DART Pipeline brings together climate and sociodemographic data as netCDF files
suitable for processing or input to machine learning models used for disease
incidence forecasting.

1. **Fetching sources**: The first stage is fetching sources from online
   providers, no or minimal processing takes place in this step. It is
   recommended that the raw sources are archived to enable reproducibly running
   the pipeline.
1. **Bias correction**: Weather data particularly from whole-Earth reanalysis
   can under- or over-estimate variables such as temperature and precipitation.
   We include a bias correction workflow for precipitation (for historical
   data) and precipitation, relative humidity and temperature (for forecast
   data).
1. **Processing sources**: Process data into netCDF files suitable for
   ingestion into a machine learning model or used for visualisation.

Each of these steps can be performed separately through the `dart-pipeline`
command line interface, and an associated utility
[`dart-bias-correct`](https://github.com/DART-Vietnam/dart-bias-correct)
available separately.

## Components

DART-Pipeline has been developed to be modular and extensible. Common code
that interfaces with APIs such as ECMWF's
[cdsapi](https://pypi.org/p/cdsapi) and zonal statistics functions (using
the [exactextract](https://pypi.org/p/exactextract) library) are in a
common utility library that can be re-used, called
[geoglue](https://geoglue.readthedocs.io). The bias correction module
([dart-bias-correct](https://github.com/DART-Vietnam/dart-bias-correct))
is in a separate repository as its dependency is GPL-3.0 licensed. For
orchestrating forecast data fetching and processing along with running
containerised models, there is a
[dart-runner](https://github.com/DART-Vietnam/dart-runner) tool. The
figure below shows the dependency relationship between these components.

```{mermaid}
graph TD
  cdsapi --> geoglue
  xarray --> geoglue
  xarray --> DART-Pipeline
  xarray --> dart-runner
  rasterio --> geoglue
  geoglue --> DART-Pipeline
  geoglue --> dart-bias-correct
  dart-bias-correct --> dart-runner
  DART-Pipeline --> dart-runner
```
