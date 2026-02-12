#!/bin/bash

set -eou pipefail

VERBOSE=0

# url list
url_list=(
    "https://github.com"
    "https://r-project.org"
    "https://data.worldpop.org"
    "https://cds.climate.copernicus.eu"
    "https://ecmwf.int"
    "https://pypi.org"
    "https://nuget.org"
)
passed=0
failed=0

# just get the header and extract the status code
# following through redirects, dump response body into /dev/null
for url in "${url_list[@]}"; do
  status=$(curl -Ls -o /dev/null -I -w '%{response_code}\n' "$url")

  if [ "$status" -eq 200 ]; then
    passed=$((passed+1))
  else
    failed=$((failed+1))
  fi
  
  if [ "$VERBOSE" -eq 1 ]; then
    echo "$url: $status"
  fi
done

echo "URLs passed: $passed. URLs failed: $failed"