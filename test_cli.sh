#!/bin/bash
# Check that the CLI does not return any errors
# This script requires an active Internet connection

set -eoux pipefail

uv run dart-pipeline list
uv run dart-pipeline get worldpop.pop_count KEN-2 2020
