# DART-Pipeline

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://github.com/kraemer-lab/DART-Pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/kraemer-lab/DART-Pipeline/actions/workflows/tests.yml)
[![Documentation status](https://readthedocs.org/projects/insightboard/badge/?version=latest)](https://insightboard.readthedocs.io/en/latest/?badge=latest)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Data analysis pipeline for the Dengue Advanced Readiness Tools (DART)
project.

The aim of this project is to develop a scalable and reproducible
pipeline for the joint analysis of epidemiological, climate, and
behavioural data to anticipate and predict dengue outbreaks.

[**Documentation**](https://dart-pipeline.readthedocs.io) | [**Contributing Guide**](CONTRIBUTING.md)

## Setup

We use [`uv`](https://docs.astral.sh/uv/getting-started/installation/)
to setup and manage Python versions and dependencies. `uv` can be
installed using the following method:
```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
```
If you do not have `curl` installed, then use `brew install curl` on
macOS or `sudo apt install curl` on Debian/Ubuntu or `sudo dnf install
curl` on Fedora/RHEL.

If you have an existing virtual environment for DART pipeline, it should
be removed as `uv` manages the `.venv` folder itself. Once installed,
you can run `dart-pipeline` as follows

```shell
git clone https://github.com/kraemer-lab/DART-Pipeline
uv sync
uv run dart-pipeline
```

## Previewing output

By default, dart-pipeline stores outputs in `~/.local/share/dart-pipeline`.
There is a [`preview.sh`](bin/preview.sh) script supplied that allows you to
preview images in the terminal by supplying an ISO3 code: `./bin/preview.sh
ISO3`. Note that this script does not generate the plots, you will have to call
`uv run dart-pipeline plot` on the output parquet files to generate those.

The preview script requires a few dependencies such as
[`chafa`](https://hpjansson.org/chafa/) or `imgcat`, an image viewer for the
terminal. A **sixel capable terminal** is also required to preview plots in the
terminal. Some terminals integrated into common editors support sixel and the
iTerm image protocol, such as Visual Studio Code. Alternatively you can use the
file manager to preview plots.

```shell
# if you have a sixel compatible terminal, you can directly see the plot in the terminal
uv run dart-pipeline plot ~/.local/share/dart-pipeline/output/VNM/worldpop/VNM-2-2020-worldpop.pop_count.parquet
# otherwise, pass the --format=png parameter to save the plot as a PNG file:
uv run dart-pipeline plot --format=png ~/.local/share/dart-pipeline/output/VNM/worldpop/VNM-2-2020-worldpop.pop_count.parquet

# We recommend trying a few plot sizes (with the --size width,height option) before running
uv run dart-pipeline plot --format=png --size 6,9 ~/.local/share/dart-pipeline/output/VNM/worldpop/VNM-2-2020-worldpop.pop_count.parquet

# Once the plot looks ok, we can generate all the plots
uv run dart-pipeline plot --format=png --size 6,9 ~/.local/share/dart-pipeline/output/VNM/**/*.parquet

# Preview generated plots
./bin/preview.sh VNM
```

## Development

Development requires the dev packages to be installed:
```shell
uv sync --all-extras
uv run pytest
```

The project uses [pre-commit hooks](https://pre-commit.com), use
`pre-commit install` to install hooks.

## Authors and Acknowledgments

- OxRSE
  - John Brittain
  - Abhishek Dasgupta
  - Rowan Nicholls
- Kraemer Group, Department of Biology
  - Moritz Kraemer
  - Prathyush Sambaturu
- Oxford e-Research Centre, Engineering Science
  - Sarah Sparrow
