# Relative wealth index

The `scripts/meta.sh` script fetches static metadata layers used for climate and
socioeconomic modeling. These include:

- Population density
- Relative wealth index

## Overview

This script is used to retrieve static data snapshots from Meta that provide
socioeconomic data for the DART Pipeline.

## Usage

```bash
./scripts/meta.sh path/to/config.sh
```

## Data storage

Data is output at `output/ISO3/meta`.
