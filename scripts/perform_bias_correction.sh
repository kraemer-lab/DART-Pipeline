#!/bin/bash
# Perform bias correction
#
# If bias correction file exists:
# * Prepare total precipitation data for bias correction
# * Perform bias correction using REMOCLIC
# * This will generate *tp_corrected* files (list format)
#
set -eou pipefail

BC_CLIP_PRECIP_PERCENTILE="${BC_CLIP_PRECIP_PERCENTILE:-0.99}"

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

BC_REPO="https://github.com/DART-Vietnam/dart-bias-correct"

command -v dart-bias-correct > /dev/null || {
    echo "==> dart-bias-correct not found, will attempt installation..."
    uv tool install "git+$BC_REPO"
}

# Commit that uv tool installed dart-bias-correct from (PEP 610 direct_url.json),
# empty if it was not installed from git
bc_installed_commit() {
    "$(uv tool dir)/dart-bias-correct/bin/python" - <<'EOF'
import importlib.metadata, json
d = json.loads(importlib.metadata.distribution("dart-bias-correct").read_text("direct_url.json") or "{}")
print(d.get("vcs_info", {}).get("commit_id", ""))
EOF
}

bc_installed=$(bc_installed_commit 2>/dev/null || true)
bc_latest=$(git ls-remote "$BC_REPO" refs/heads/main 2>/dev/null | cut -f1 || true)
if [ -z "$bc_installed" ]; then
    echo "==> dart-bias-correct not installed from git, skipping version check"
elif [ -z "$bc_latest" ]; then
    echo "==> Could not fetch latest dart-bias-correct commit, skipping version check"
elif [ "$bc_installed" != "$bc_latest" ]; then
    echo "==> dart-bias-correct is at ${bc_installed:0:7}, main is at ${bc_latest:0:7}, upgrading..."
    uv tool install --reinstall "git+$BC_REPO@main"
fi

if [ -z "$BC_PRECIP_REF" ]; then
    echo "PRECIP_REF (precipitation reference dataset) is not set, cannot perform bias correction"
    exit 1
fi

# TODO: Prepare total precipitation data for bias correction [OPTIONAL]

# Perform bias correction using $BC_PRECIP_REF, this will generate tp_corrected
# files
test -f "$BC_HISTORICAL_OBS" || die "==> Historical observation file not found: $BC_HISTORICAL_OBS"
test -f "$BC_PRECIP_REF" || die "==> Reference precipitation file not found: $BC_PRECIP_REF"

"$(dirname "$0")/remove_superseded_part_files.sh" "$1"

echo -e "\033[1m==> Performing bias correction\033[0m"
# shellcheck disable=SC2154
for ((i=_fetch_start_year;i<=_fetch_end_year;i++))
do
  dart-bias-correct precipitation "$BC_PRECIP_REF" "$BC_HISTORICAL_OBS" \
      "$ISO3-$i" --clip-precip-percentile="$BC_CLIP_PRECIP_PERCENTILE"
done
