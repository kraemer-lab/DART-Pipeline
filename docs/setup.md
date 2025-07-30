# Setup

## Pre-requisites

The supported platforms for DART Pipeline are macOS (Apple Silicon and 64-bit) and Linux 64-bit. Windows users should use WSL.

The [Climate Data Operators](https://code.mpimet.mpg.de/projects/cdo) utility
is used for resampling source raster netCDF files. You can install it as
follows:

```shell
brew install cdo  # on macOS
sudo apt install cdo  # on Ubuntu/Debian
```

We use [uv](https://docs.astral.sh/uv/getting-started/installation/) to setup
and manage Python versions and dependencies. uv can be installed using the
following method:

```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If you do not have curl installed, then use `brew install curl` on macOS or
`sudo apt install curl` on Debian/Ubuntu.

## Installation

Clone the repository and use `uv` to run the main pipeline command-line tool.

```shell
git clone https://github.com/kraemer-lab/DART-Pipeline
uv sync
uv run dart-pipeline
```

This will print out the commands available, such as **get** to fetch data, and
**process** to process data. The output of this command also shows the default
directory where source files are downloaded and processed output files are
written to. Usually this is `~/.local/share/dart-pipeline`.

To see the help for any command e.g. get, type

```shell
uv run dart-pipeline get --help
```

While dart-pipeline provides a granular way to fetch and process data from
multiple sources, we also offer a streamlined method to run the pipeline
through a single [configuration file](workflow/configuration), and a set of
shell scripts.
