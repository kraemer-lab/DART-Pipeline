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

Testing
=======
- Clone the repository: `git clone git@github.com:kraemer-lab/DART-Pipeline.git`
- Create a `credentials.json` file in the newly-cloned `DART-Pipeline` folder
- `cd` into the A folder and run the tests:
    - `python3 test_collate_data.py`
    - `python3 test_utils.py`
- Still in the A folder, collate the data needed to test the B scripts:
    - `python3 collate_data.py GADM`
    - `python3 collate_data.py "APHRODITE precipitation"`
    - `python3 collate_data.py "APHRODITE temperature"`
    - `python3 collate_data.py "CHIRPS rainfall"`
    - `python3 collate_data.py "ERA5 reanalysis"`
    - `python3 collate_data.py "TerraClimate data"`
    - `python3 collate_data.py "WorldPop pop density"`
    - `python3 collate_data.py "WorldPop pop count"`
- `cd` into the B folder and run the tests:
    - `python3 test_process_data.py`
    - `python3 test_utils.py`

Timeline
========

+------------+---------------------------------------------------+
| Date       | Task                                              |
+============+===================================================+
| 2024-11    | Version 1 release. Project maturing after this    |
|            | point.                                            |
+------------+---------------------------------------------------+

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
