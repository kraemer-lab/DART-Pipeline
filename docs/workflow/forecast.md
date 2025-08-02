# Forecast

The forecast processing workflow is contained in a separate
[`dart-runner`](https://github.com/DART-Vietnam/dart-runner) repository in a
script [`main.py`](https://github.com/DART-Vietnam/dart-runner/blob/main/main.py). First, clone the dart-runner repository
and use `uv` to install:
```shell
git clone https://github.com/DART-Vietnam/dart-runner
uv sync
```

To fetch forecast data for the current date:
```shell
uv run main.py
```

As there is no way of subsetting the downloaded data, forecast data is
downloaded for the entire world -- this requires about 8 GB of space. The data
is cropped to the selected region, so this file can be deleted afterwards from
the `~/.local/share/dart-pipeline/sources/WLD/ecmwf` folder.

To fetch forecast data for a particular date (at most 4 days in the past):
```shell
uv run main.py --date=YYYY-MM-DD
```

The full pipeline calculates SPI and SPEI calculations to be performed which
requires the last five weeks of data (for a window of 6 weeks). As ERA5
provides data with a lag of five days, we use the previous week's forecast
after bias correction as a pseudo-observation, concatenating it with 4 weeks of
ERA5 data:

```{mermaid}
gantt
    dateFormat  YYYY-MM-DD
    title ERA5 SPI/SPEI calculation
    tickInterval 1week
    weekday monday

    section ERA5 (Past 4 Weeks)
    Week -4     :done,    past1, 2025-01-27, 7d
    Week -3     :done,    past2, after past1, 7d
    Week -2     :done,    past3, after past2, 7d
    Week -1     :done,    past4, after past3, 7d

    section Previous Week's Forecast (bias corrected)
    Week 0: forecast1, after past4, 7d

    section 2-Week Forecast (bias corrected)
    Week 1     :active, forecast2, after forecast1, 7d
    Week 2     :active, forecast3, after forecast2, 7d
```

Without previous week's forecast data availability, the pipeline will run and
provide zonal aggregated statistics for variables other than `spei_bc` and
`spi_bc`.

## Running models

There is a machine learning model in development. For demonstration purposes,
we provide a dummy Dengue incidence "forecast"; the `main.py` script provides
code to call a Docker container that can be adapted to run user defined models.
