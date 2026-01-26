#!/bin/bash

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename "$0") config.sh"
    exit 1
fi

# shellcheck disable=SC1090
. "$1"

# Get current year and subtract 2
_current_year=$(date +%Y)
_last_supported_year=$((_current_year))

# shellcheck disable=SC2154
if [ "$_fetch_start_year" -gt "$_fetch_end_year" ]; then
  echo error: start year must precede end year
  exit 1
fi

if [ "$_fetch_end_year" -gt "$_last_supported_year" ]; then
  echo error: end year must not be in future
fi

if [ "$_fetch_end_year" -eq "$_last_supported_year" ]; then
  echo warn: data for the current year will be partial
fi

echo -e "\033[1m==> Fetching and processing population data\033[0m"
for ((i=_fetch_start_year;i<=_fetch_end_year;i++))
do
  uv run dart-pipeline get worldpop.pop_count "$ISO3-$ADMIN" "$i"
done

for ((i=_fetch_start_year;i<=_fetch_end_year;i++))
do
  echo -e "\033[1m==> Fetching weather data for $i\033[0m"
  uv run dart-pipeline get era5 "$ISO3" "$i"
done
