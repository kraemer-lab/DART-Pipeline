*******
Metrics
*******

For metrics with a 'partOf' attribute, the metric is calculated by invoking
dart-pipeline (get|process) on the 'partOf' value rather than the metric value.
This is usually done for efficiency reasons when it is faster to process
multiple metrics at once.

To fetch data for a particular metric, run

.. code-block::

   uv run dart-pipeline get metric ISO3-admin YYYY param=value

where ``param`` and ``value`` refer to optional parameters and their values
that are passed directly to the appropriate function. The first parameter
specifies the ISO3 code of the country and the administrative level (1, 2, or
3) to aggregate data to. The second parameter is usually the year (can also be
the date) for which to download and process data. If a processor exists, it is
invoked automatically, or can be manually invoked by:

.. code-block::

   uv run dart-pipeline process metric ISO3-admin YYYY param=value

Note that weather data from the ``era5`` source is not automatically processed
as fetching takes a long time. For this case run the get and process steps
separately.


WorldPop population data - ``worldpop``
===========================================================

worldpop.pop_count
    WorldPop population count [unitless]

    :URL: https://hub.worldpop.org/geodata/listing?id=75
    :License: CC-BY-4.0
    :Citation:
      WorldPop (www.worldpop.org - School of Geography and Environmental
      Science, University of Southampton; Department of Geography and
      Geosciences, University of Louisville; Departement de Geographie,
      Universite de Namur) and Center for International Earth Science
      Information Network (CIESIN), Columbia University (2018). Global
      High Resolution Population Denominators Project - Funded by The
      Bill and Melinda Gates Foundation (OPP1134076).
      https://dx.doi.org/10.5258/SOTON/WP00671

Sociodemographic indicators from Meta - ``meta``
===========================================================

meta.pop_density
    Meta population density [unitless]

    :URL: https://dataforgood.facebook.com/dfg/docs/high-resolution-population-density-maps-demographic-estimates-documentation
    :Citation:
      Facebook Connectivity Lab and Center for International Earth Science
      Information Network - CIESIN – Columbia University. 2016.
      High Resolution Settlement Layer (HRSL). Source imagery for
      HRSL © 2016 DigitalGlobe. Accessed YYYY-MM-DD

meta.relative_wealth_index
    Relative wealth index [unitless]

    :URL: https://dataforgood.facebook.com/dfg/tools/relative-wealth-index
    :License: CC-BY-NC-4.0
    :Citation:
      Microestimates of wealth for all low- and middle-income countries.
      Guanghua Chi, Han Fang, Sourav Chatterjee, Joshua E. Blumenstock.
      Proceedings of the National Academy of Sciences Jan 2022, 119 (3)
      e2113658119; DOI: 10.1073/pnas.2113658119

ERA5 reanalysis data - ``era5``
===========================================================
:Authentication: Authentication required, see https://cds.climate.copernicus.eu/how-to-api
:License:
    Access to Copernicus Products is given for any purpose in so far
    as it is lawful, whereas use may include, but is not limited to: reproduction;
    distribution; communication to the public; adaptation, modification and
    combination with other data and information; or any combination of the
    foregoing.

era5.2m_temperature
    2 meters air temperature [K], *partOf* era5


era5.surface_solar_radiation_downwards
    Accumulated solar radiation downwards [J/m^2], *partOf* era5


era5.total_precipitation
    Total precipitation [m], *partOf* era5


era5.wind_speed
    Wind speed [m/s], *partOf* era5


era5.relative_humidity
    Relative humidity [percentage], *partOf* era5


era5.specific_humidity
    Specific humidity [g/kg], *partOf* era5


era5.hydrological_balance
    Hydrological balance [m], *partOf* era5


era5.spi
    Standardised precipitation [unitless], *partOf* era5


era5.spei
    Standardised precipitation-evaporation index [unitless], *partOf* era5


era5.total_precipitation_corrected
    Bias-corrected total precipitation [m], *partOf* era5


era5.spi_corrected
    Bias-corrected standardised precipitation [unitless], *partOf* era5


era5.spei_corrected
    Bias-corrected standardised precipitation-evaporation index [unitless], *partOf* era5


era5.hydrological_balance_corrected
    Bias-corrected hydrological balance [m], *partOf* era5


era5.spi.gamma
    Fitted gamma distribution from historical data for SPI [unitless]


era5.spei.gamma
    Fitted gamma distribution from historical data for SPEI [unitless]


era5.spi_corrected.gamma
    Fitted gamma distribution from historical data for SPI with corrected precipitation [unitless]


era5.spei_corrected.gamma
    Fitted gamma distribution from historical data for SPEI with corrected precipitation [unitless]


era5.prep_bias_correct
    Virtual metric to prepare aggregated data for bias correction module [various]
