#!/bin/bash
# Fetch and process WorldPop age and sex structures
#
# Optional sex= and age= parameters are passed to
# `dart-pipeline get worldpop.age_sex_structure`, for example
#
#   fetch_age_sex.sh config.sh sex=t age=0-5,65+
#
# Without them, all age groups are fetched for female and male (sex=f,m)
#

set -eou pipefail

usage="usage: $(basename "$0") config.sh [sex=f,m,t] [age=0-5,65+]"
if [ "$#" -lt 1 ]; then
    echo "$usage"
    exit 1
fi

_config="$1"
shift

_params=()
for arg in "$@"; do
    case "$arg" in
        sex=*|age=*) _params+=("$arg") ;;
        *)
            echo "error: unknown parameter '$arg', only sex= and age= are supported"
            echo "$usage"
            exit 1
            ;;
    esac
done

# shellcheck disable=SC1090
. "$_config"

echo -e "\033[1m==> Fetching and processing age-sex-structured population data${_params[*]+ ${_params[*]}}\033[0m"
# shellcheck disable=SC2154
for ((i=_fetch_start_year;i<=_fetch_end_year;i++))
do
  # ${arr[@]+...} expands an empty array safely under set -u in bash < 4.4
  uv run dart-pipeline get worldpop.age_sex_structure "$ISO3-$ADMIN" "$i" ${_params[@]+"${_params[@]}"}
done
