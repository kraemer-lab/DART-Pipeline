#!/bin/bash
# Preview plots in the terminal
# Requires a sixel compatible terminal: https://www.arewesixelyet.com/
# Depends: chafa  -- to display sixel images
# Depends: bat    -- to display files syntax coloured
# Depends: imgcat -- optional, to display images in iTerm2

if [ -z "$1" ]; then
    cat << EOF
usage: preview.sh ISO3 [DATA_PATH] [FZF_ARGS]

ISO3 - Country ISO 3166-1 alpha-3 code
DATA_PATH - Data path to preview files in (default=~/.local/share/dart-pipeline)
FZF_ARGS - Passed to fzf, e.g. --preview-window=up to shift the preview window
EOF
    exit 2
fi


ISO3="$1"
shift
DATA_PATH="${DART_DATA_PATH:-$HOME/.local/share/dart-pipeline}"
FZF_PREVIEW="$(dirname "$0")/fzf-preview.sh"

if [ -z "$ISO3" ]; then
    cat << EOF
usage: preview.sh ISO3 [FZF_ARGS]

ISO3 - Country ISO 3166-1 alpha-3 code
DATA_PATH - Data path to preview files in (default=~/.local/share/dart-pipeline)
FZF_ARGS - Passed to fzf, e.g. --preview-window=up to shift the preview window
EOF
    exit 2
fi

OUTPUT_DIR="${DATA_PATH}/output/$ISO3"

if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Missing data for country ISO3: $ISO3"
    exit 1
fi

find "$OUTPUT_DIR" -name '*.png' | sed "s|$OUTPUT_DIR/||g" | fzf --preview="$FZF_PREVIEW $OUTPUT_DIR/{}" $@
