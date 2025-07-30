# Bias correction

The `scripts/perform_bias_correction.sh` script applies bias correction to
precipitation data using the `dart-bias-correct` tool. This process adjusts
modeled precipitation values to better match historical observations.

## Overview

This script prepares and corrects total precipitation data for each year in the
configured time range. It requires a valid precipitation reference file,
historical observations, and a configuration that enables bias correction.

If bias correction is not enabled in the configuration, the script exits early
without performing any operations.

## Usage

```bash
./scripts/perform_bias_correction.sh config.sh
```

## Data access

Some of the reference datasets for bias correction are not open access. This
includes the historical forecast data obtained from ECMWF's MARS service.

The historical observation data can be generated using `dart-pipeline`:
```shell
uv run dart-pipeline process era5.prep_bias_correct ISO3 Y1-Y2
```

## Data storage

Running bias correction creates `ISO3-YYYY-era5.accum.tp_corrected.nc` files in
the `sources/ISO3/era5` subfolder. These are picked up by the processing
pipeline.
