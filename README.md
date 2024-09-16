# DART-Pipeline

Data analysis pipeline for the Dengue Advanced Readiness Tools (DART)
project.

The aim of this project is to develop a scalable and reproducible
pipeline for the joint analysis of epidemiological, climate, and
behavioural data to anticipate and predict dengue outbreaks.

[**Contributing Guide**](CONTRIBUTING.md)

## Setup

We use [`uv`](https://docs.astral.sh/uv/getting-started/installation/)
to setup and manage Python versions and dependencies. Once installed,
you can run `dart-pipeline` as follows

```shell
git clone https://github.com/kraemer-lab/DART-Pipeline
uv sync
uv run dart-pipeline
```

## Data sources

To see a list of data sources known to `dart-pipeline` and the
associated processing steps:

```shell
> uv run dart-pipeline list
  source economic/relative-wealth-index
  source epidemiological/dengue/peru
  source geospatial/gadm
  source meteorological/aphrodite-daily-mean-temp
  source meteorological/aphrodite-daily-precip
  source meteorological/chirps-rainfall
  source meteorological/terraclimate
  source sociodemographic/meta-pop-density
  source sociodemographic/worldpop-count
  source sociodemographic/worldpop-density
 process economic/relative-wealth-index
 process epidemiological/dengue/peru
 process geospatial/chirps-rainfall
 process geospatial/gadm
 process geospatial/worldpop-count
 process meteorological/aphrodite-daily-mean-temp
 process meteorological/aphrodite-daily-precip
 process meteorological/chirps-rainfall
 process meteorological/era5-reanalysis
 process sociodemographic/worldpop-count
 process sociodemographic/worldpop-density
```

To get data for a source, run `dart-pipeline get`:

```shell
uv run dart-pipeline get geospatial/gadm iso3=VNM
```

Data is downloaded into the `data/sources` folder under the root of the
repository by default. Here, the data will be downloaded into the
`data/sources/geospatial/gadm/VNM` folder.

## Data processing

Once data is downloaded into `data/sources`, you can run the processing
steps:

```shell
uv run dart-pipeline process geospatial/gadm iso3=VNM admin_level=2
```

Each processing step takes certain parameters such as `iso3`. To see the
list of required parameters for a processing step, run the command
without any parameters

```shell
> uv run dart-pipeline process geospatial/gadm
❗ geospatial/gadm missing required parameters {'iso3', 'admin_level'}
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
