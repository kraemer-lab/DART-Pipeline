# Calculating standardised indices

DART-Pipeline can calculate standardised indices for precipitation -
specifically the Standardised Precipitation Index (SPI) and the Standardised
Precipitation Evaporation Index (SPEI). While the core weather processing
(daily aggregation and zonal statistics) can be performed on a yearly basis,
calculation of SPI and SPEI involves additional steps.

```{note}
You can run the entire historical ERA5 reanalysis weather pipeline:

    uv run dart-pipeline process era5 ISO3 2000-2020

after fetching data for each of the years:

    uv run dart-pipeline get era5 ISO3 YEAR

This page is useful if you want to separate out SPI and SPEI calculations
or invoke them manually, e.g. after updating bias corrected data
```

## Estimating the gamma distribution

SPI and SPEI are represented as a dimensionless number, usually in the -3 to 3
range. The first step to calculate SPI or SPEI is to estimate the gamma
distribution from a representative timeseries dataset of precipitation, which
should be at least 15 years. Once there is enough weather data, one can estimate the gamma parameters

```
uv run dart-pipeline process era5.spi.gamma VNM 2000-2020
```

By default a window of 6 weeks is used to estimate SPI and SPEI; this is as SPI
and SPEI are calculated as rolling means over a window after weekly aggregation
(summation) of precipitation values. To override the window parameter, pass
`window=n` to the above command. The gamma distribution is estimated at each
longitude and latitude grid point and saved as a netCDF file:

```
> ncdump -h output/VNM/era5/VNM-era5.spi.gamma.nc
netcdf VNM-era5.spi.gamma {
dimensions:
	longitude = 33 ;
	latitude = 65 ;
variables:
	float longitude(longitude) ;
		longitude:_FillValue = NaNf ;
		longitude:units = "degrees_east" ;
		longitude:long_name = "longitude" ;
	float latitude(latitude) ;
		latitude:_FillValue = NaNf ;
		latitude:units = "degrees_north" ;
		latitude:long_name = "latitude" ;
	double alpha(latitude, longitude) ;
		alpha:_FillValue = NaN ;
	double beta(latitude, longitude) ;
		beta:_FillValue = NaN ;

// global attributes:
		:DART_history = "gamma_spi(\'VNM\', reference=\'daily_tp_era5.nc\', window=6)" ;
		:ISO3 = "VNM" ;
		:metric = "era5.spi.gamma" ;
}
```

DART saves the parameters for the estimation of the gamma distribution. This is used to calculate the SPI or SPEI in the next step.

## Calculating SPI

Once gamma parameters are estimated, the SPI can be calculated as follows for a particular year, e.g. 2001:

```
uv run dart-pipeline process era5.spi 2001
```

Replace `spi` with `spei` above to perform the same calculations for SPEI.

## Calculating SPI and SPEI with bias correction

The above commands calculate SPI and SPEI with uncorrected precipitation. To do
the same for corrected precipitation, first ensure that you have the corrected
precipitation files for each year; then run the above commands with the
`bias_correct` flag, e.g.

```
uv run dart-pipeline process era5.spi.gamma VNM 2000-2020 bias_correct
```
