#!/bin/bash

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename "$0") config.sh"
    exit 1
fi

# shellcheck disable=SC1090
. "$1"

echo "==> Meta relative wealth index is a fixed snapshot, disregarding years"
uv run dart-pipeline get meta.relative_wealth_index "$ISO3-$ADMIN"
