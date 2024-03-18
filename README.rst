=============
DART-Pipeline
=============
Data analysis pipeline for the Dengue Advanced Readiness Tools (DART) project.

The aim of this project is to develop a scalable and reproducible pipeline for the joint analysis of epidemiological, climate, and behavioural data to anticipate and predict dengue outbreaks.

Project Manifest
================

Input
-----
- See `DART dataset summarisation.xls <https://unioxfordnexus.sharepoint.com/:x:/r/sites/EngineeringScience-DART/Shared%20Documents/General/DART%20dataset%20summarisation.xlsx?d=w2e772ccb5717440ab47790a6b733a73b&csf=1&web=1&e=Eapex6&nav=MTJfTjNfezAwMDAwMDAwLTAwMDEtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMH0>`_

Intermediate
------------
1. Raw data that has been downloaded is found in folder "A Collate Data"
2. Collated data that has been processed is found in folder "B Process Data"

Output
------
TBD

Documentation
-------------
- Located here: `~/DART-Pipeline/docs/build/singlehtml/index.html`
- Created using Sphinx
- Generated with:

.. code-block::

    $ cd ~/DART-Pipeline/docs
    $ make singlehtml

Progress
========
collate_data.py

- download_epidemiological_data()
    - ✅ download_ministerio_de_salud_peru_data()
- download_geospatial_data()
    - ✅ download_gadm_admin_map_data()
- download_meteorological_data()
    - ✅ download_aphrodite_precipitation_data()
    - ✅ download_aphrodite_temperature_data()
    - ✅ download_chirps_rainfall_data()
    - ✅ download_era5_reanalysis_data()
    - ✅ download_terraclimate_data()
- download_socio_demographic_data()
    - ✅ download_worldpop_pop_density_data()
    - ✅ download_worldpop_pop_count_data()

process_data.py

- process_geospatial_data()
    - ✅ process_gadm_admin_map_data()
- process_meteorological_data()
    - ✅ process_aphrodite_precipitation_data()
    - ✅ process_aphrodite_temperature_data()
    - ✅ process_chirps_rainfall_data()
    - ✅ process_era5_reanalysis_data()
    - ✅ process_terraclimate_data()
- process_socio_demographic_data()
    - ✅ process_worldpop_pop_count_data()
    - ✅ process_worldpop_pop_density_data()
- process_geospatial_sociodemographic_data()
    - ✅ process_gadm_worldpoppopulation_data()


Contributing
============
Please see our `Contributing to DART <./CONTRIBUTING.md>`_ guide.

Authors and Acknowledgments
===========================
- OxRSE
    - John Brittain
    - Rowan Nicholls
- Kraemer Group, Department of Biology
    - Moritz Kraemer
    - Prathyush Sambaturu
- Oxford e-Research Centre, Engineering Science
    - Sarah Sparrow
