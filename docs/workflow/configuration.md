# Configuration

While `dart-pipeline` can process individual data sources, for convenience, we
provide a
[`config.sh`](https://github.com/kraemer-lab/DART-Pipeline/blob/main/config.sh)
that can be used by a set of [Bash
scripts](https://github.com/kraemer-lab/DART-Pipeline/tree/main/scripts) to run
the pipeline. This simplifies calling the pipeline by allowing all the options
to be defined in one place. The configuration file has the following settings.
that are used for data selection, resolution, and optional bias correction
during pipeline execution.

## Configuration Parameters

ISO3
: Three-letter ISO country code.
  **Example**: `VNM` (Vietnam)

ADMIN
: Administrative level for spatial aggregation.
  **Options**: `1`, `2`, or `3`

TEMPORAL_RESOLUTION
: Time resolution for processing core climate parameters such as temperature (`t2m`), relative humidity (`r`), and maximum temperature (`mx2t24`).
  **Options**: `daily`, `weekly`
  **Default**: `weekly`
  Note: SPI and SPEI are always computed at a weekly resolution. At daily resolution, outputs are not concatenated into single files due to resolution mismatches.

START_YEAR
: First year of the study period.
  **Example**: `2001`

END_YEAR
: Last year of the study period.
  **Example**: `2019`
  Note: Data is also fetched one year before and after this period to align with ISO week requirements.

INDEX_START_YEAR
: Start year for SPI and SPEI gamma parameter estimation.
  Defaults to `START_YEAR`, can be overridden if necessary for statistical robustness.

INDEX_END_YEAR
: End year for SPI and SPEI gamma parameter estimation.
  Defaults to `END_YEAR`.

BC_ENABLE
: Enables or disables bias correction.
  **Values**: `1` (enabled), anything else disables it.

BC_PRECIP_REF
: Reference dataset for total precipitation, used in bias correction.
  **Example**: Dataset such as [REMOCLIC. 2016](https://search.diasjp.net/en/dataset/VnGP_010) that provides a reference precipitation dataset (for Vietnam)

BC_CLIP_PRECIP_PERCENTILE
: Percentile at which reference data is clipped for precipitation bias correction. This
  is done to reduce the effect of outliers in bias correction using quantile mapping.
  **Default**: 0.99

BC_HISTORICAL_FORECAST
: Historical forecast dataset from ECMWF MARS service.
  **Notes**: This is used for correcting forecast data. This data is not open-access.

BC_HISTORICAL_OBS
: Historical observation dataset used for bias correction. Must include:
  - `t2m`: 2-meter air temperature
  - `r`: Relative humidity
  - `tp`: Total precipitation
  **Notes**: This can be generated using `uv run dart-pipeline process era5.prep_bias_correct`


Note that data is always fetched one year prior to and one year after the
included period (`_fetch_start_year` and `_fetch_end_year` in the script). This
is so that we can perform ISO week alignment for calculation of standardised
indices such as SPI and SPEI.

Once the configuration is setup, individual scripts can be called as follows

```shell
bash scripts/scriptname.sh config.sh
```
This will read variables from the configuration file
