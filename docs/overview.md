# Overview

DART Pipeline processes data in two stages:

1. **Fetching sources**: The first stage is fetching sources from online
   providers, no or minimal processing takes place in this step. It is
   recommended that the raw sources are archived to enable reproducibly running
   the pipeline.

2. **Processing sources**: Process data into a format suitable for ingestion
   into a database, or as flat files, to be read by downstream visualisation
   and analytics pipelines.

Each of these steps can be performed separately through the `dart-pipeline` command line interface.

## Data sources

Data sources are organised in a two level categorical hierarchy, with the top
level denoting the type of data: *meterological*, *epidemiological*, *geospatial* and
*sociodemographic*.

To see a list of data sources known to `dart-pipeline` and the associated
processing steps (example output, actual output may differ):

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
