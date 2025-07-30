# Processing data

The `scripts/process_weather.sh` script pprocesses weather data for a configured
region and time range. This includes:

- Fitting gamma parameters for SPI and SPEI
- Running SPI and SPEI calculations
- Processing core weather variables at weekly or daily resolution
- Optionally applying bias correction
- Concatenating output when using weekly resolution

## Overview

This script serves as the main processor in the climate data pipeline. It reads configuration options, determines whether bias correction should be applied, and uses `dart-pipeline` to generate final weather datasets.

## Usage

```bash
./scripts/process_weather.sh path/to/config.sh
```

## Data storage

The processing pipeline creates the following outputs:

- `output/ISO3/era5`: Zonally aggregated dataset for weather variables,
  including SPI and SPEI. If at weekly resolution, these files are concatenated
  into one file: `ISO3-ADMIN-Y1-YN-era5.nc`.
- `output/ISO3/worldpop`: Population counts, zonally aggregated, stored as
  `ISO3-ADMIN-YYYY-worldpop.pop_count.nc`, one for each year.
