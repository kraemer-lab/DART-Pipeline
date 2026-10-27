#!/bin/bash
# Remove superseded ERA5 partial month files
#
# Remove _part files that have been superseded by a complete month download,
# otherwise DatasetPool fails when both exist for the same month (see GH issue #340)
#
# Called by perform_bias_correction.sh and process_weather.sh
#

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename "$0") config.sh"
    exit 1
fi

# shellcheck disable=SC1090
. "$1"

_stub_dir="${XDG_DATA_HOME:-$HOME/.local/share}/dart-pipeline" # change this if needed

_era5_dir="${DART_PIPELINE_DATA_HOME:-$_stub_dir}/sources/$ISO3/era5"
for _part_file in "$_era5_dir"/*_part-*; do
    [ -e "$_part_file" ] || continue
    _name="${_part_file##*/}"
    if [ -e "$_era5_dir/${_name/_part-/-}" ]; then
        echo "removing superseded partial file: $_part_file"
        rm "$_part_file"
    fi
done
