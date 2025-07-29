#!/bin/bash
# Process weather data
#
# Fit SPI and SPEI gamma parameters
# Run SPI and SPEI calculations for each year from start_year to end_year
#
# Run rest of the processing (core.weekly or core.daily)
# Concatenate output into one file (if weekly timestep)
#

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename "$0") config.sh"
    exit 1
fi

# shellcheck disable=SC1090
. "$1"

_res="${TEMPORAL_RESOLUTION:-weekly}"

if [ "$_res" != "daily" ] &&  [ "$_res" != "weekly" ]; then
    echo "error: invalid TEMPORAL_RESOLUTION, must be one of 'daily' or 'weekly'"
    exit 1
fi

if [ "$BC_ENABLE" -eq 1 ]; then
    uv run dart-pipeline process era5 "$ISO3-$ADMIN" "${START_YEAR}-${END_YEAR}" \
        temporal_resolution="$TEMPORAL_RESOLUTION" overwrite
else
    uv run dart-pipeline process era5 "$ISO3-$ADMIN" "${START_YEAR}-${END_YEAR}" \
        temporal_resolution="$TEMPORAL_RESOLUTION" overwrite skip_correction
fi
