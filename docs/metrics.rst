Metrics
=======

DART Pipeline supports metrics in these categories: economic, epidemiological,
meteorological and sociodemographic. Following are the list of metrics, where ↓
denotes fetch (collate) code and ↺ denotes processing code. An asterisk (*)
denotes metrics which require an account to be created.

- economic/relative-wealth-index
  :func:`↓ <dart_pipeline.collate.relative_wealth_index>`
  :func:`↺ <dart_pipeline.process.process_rwi>`
- epidemiological/dengue/peru
  :func:`↓ <dart_pipeline.collate.ministerio_de_salud_peru_data>`
  :func:`↺ <dart_pipeline.process.process_ministerio_de_salud_peru_data>`
- geospatial/gadm
  :func:`↓ <dart_pipeline.collate.gadm_data>`
  :func:`↺ <dart_pipeline.process.process_gadm_admin_map_data>`
- meteorological/aphrodite-daily-mean-temp *
  :func:`↓ <dart_pipeline.collate.aphrodite_temperature_data>`
  :func:`↺ <dart_pipeline.process.process_aphrodite_temperature_data>`
- meteorological/aphrodite-daily-precip *
  :func:`↓ <dart_pipeline.collate.aphrodite_precipitation_data>`
  :func:`↺ <dart_pipeline.process.process_aphrodite_precipitation_data>`
- meteorological/chirps-rainfall
  :func:`↓ <dart_pipeline.collate.chirps_rainfall_data>`
  :func:`↺ <dart_pipeline.process.process_chirps_rainfall>`
- meteorological/terraclimate
  :func:`↓ <dart_pipeline.collate.terraclimate_data>`
  :func:`↺ <dart_pipeline.process.process_terraclimate>`
- meteorological/era5-reanalysis
  :func:`↺ <dart_pipeline.process.process_era5_reanalysis_data>`
- sociodemographic/meta-pop-density
  :func:`↓ <dart_pipeline.collate.meta_pop_density_data>`
  :func:`↺ <dart_pipeline.process.process_ministerio_de_salud_peru_data>`
- sociodemographic/worldpop-count
  :func:`↓ <dart_pipeline.collate.worldpop_pop_count_data>`
  :func:`↺ <dart_pipeline.process.process_worldpop_pop_count_data>`
- sociodemographic/worldpop-density 
  :func:`↓ <dart_pipeline.collate.worldpop_pop_density_data>`
  :func:`↺ <dart_pipeline.process.process_worldpop_pop_density_data>`
- geospatial/chirps-rainfall 
  :func:`↺ <dart_pipeline.process.process_gadm_chirps_rainfall>`
- geospatial/worldpop-count
  :func:`↺ <dart_pipeline.process.process_gadm_admin_map_data>`

To fetch data for a particular metric, run

.. code-block::

   uv run dart-pipeline get metric param=value

where ``param`` and ``value`` refer to parameters and their values that are
passed directly to the appropriate function. As an example if a collate
function takes the parameter ``iso3: str``, then this should be specified on
the command line as ``uv run dart-pipeline get metric iso3=<ISO 3166-2 alpha-3
code>``

To process data for a particular metric run

.. code-block::

    uv run dart-pipeline process metric param=value


Collate functions
-----------------

.. automodule:: dart_pipeline.collate
    :members:

Process functions
-----------------

.. automodule:: dart_pipeline.process
    :members:
