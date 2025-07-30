# Fetching data

This can be performed using the `scripts/fetch_weather.sh` script or by running
`uv run dart-pipeline get` with one of the supported [data processors](reference/variables).

The script fetches and prepares population and weather data for a specified ISO country code and administrative level, using a provided configuration file.

## Overview

This script is designed to automate the retrieval of:

- **Population data** using the `worldpop.pop_count` dataset.
- **Weather data** from the ERA5 dataset.

The script determines the year range based on the configuration and ensures all required years (including padding years for ISO week alignment) are available.

## Usage

```bash
./scripts/fetch_weather.sh path/to/config.sh
```

```{note}
Access to ERA5 reanalysis data requires a Climate Data Store account, see more information
at https://cds.climate.copernicus.eu/how-to-api
```

## Data storage

Data will be stored at `~/.local/share/dart-pipeline/sources/ISO3/era5` for ERA5 data. Data is downloaded as GRIB files and converted to netCDF files locally. The processing code automatically timeshifts downloaded ERA5 hourly data according to the detected timezone (e.g. UTC+08:00 for Singapore).
