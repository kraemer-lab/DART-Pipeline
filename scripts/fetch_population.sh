#!/bin/bash
# Fetch and process WorldPop population counts
#
# Fetches worldpop.pop_count for each year from _fetch_start_year to
# _fetch_end_year, and processes it to "$ISO3-$ADMIN"
#

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename "$0") config.sh"
    exit 1
fi

# shellcheck disable=SC1090
. "$1"

echo -e "\033[1m==> Fetching and processing population data\033[0m"
# shellcheck disable=SC2154
for ((i=_fetch_start_year;i<=_fetch_end_year;i++))
do
  uv run dart-pipeline get worldpop.pop_count "$ISO3-$ADMIN" "$i"
done
