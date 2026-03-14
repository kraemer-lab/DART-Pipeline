#!/bin/bash

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

uv run dart-pipeline process wrf_downscale.precip "$ISO3-$ADMIN" \
  "${START_YEAR}-${END_YEAR}" temporal_resolution="$TEMPORAL_RESOLUTION"