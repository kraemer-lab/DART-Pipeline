#!/bin/bash
# Perform bias correction
#
# If bias correction file exists:
# * Prepare total precipitation data for bias correction
# * Perform bias correction using REMOCLIC
# * This will generate *tp_corrected* files (list format)
#
set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename "$0") config.sh"
    exit 1
fi

# shellcheck disable=SC1090
. "$1"

die() {
    echo "$*"
    exit 1
}

if [ "${BC_ENABLE:-0}" -ne 1 ]; then
    echo "==> Bias correction turned off, set BC_ENABLE=1 and reforence files to enable"
    exit 0
fi

command -v uv > /dev/null || die "==> uv not installed, visit https://astral.sh/uv for installation instructions"

command -v dart-bias-correct > /dev/null || {
    echo "==> dart-bias-correct not found, will attempt installation..."
    uv tool install git+https://github.com/DART-Vietnam/dart-bias-correct
}

if [ -z "$BC_PRECIP_REF" ]; then
    echo "PRECIP_REF (precipitation reference dataset) is not set, cannot perform bias correction"
    exit 1
fi

# TODO: Prepare total precipitation data for bias correction [OPTIONAL]

# Perform bias correction using $BC_PRECIP_REF, this will generate tp_corrected
# files
test -f "$BC_HISTORICAL_OBS" || die "==> Historical observation file not found: $BC_HISTORICAL_OBS"
test -f "$BC_PRECIP_REF" || die "==> Reference precipitation file not found: $BC_PRECIP_REF"

echo -e "\033[1m==> Performing bias correction\033[0m"
# shellcheck disable=SC2154
for ((i=_fetch_start_year;i<=_fetch_end_year;i++))
do
  echo dart-bias-correct precipitation "$BC_PRECIP_REF" "$BC_HISTORICAL_OBS" "$ISO3-$i"
done
