=============
DART-Pipeline
=============

README
======
The aim of this project is to develop a scalable and reproducible pipeline for the joint analysis of epidemiological, climate, and behavioural data to anticipate and predict dengue outbreaks. 

Project Manifest
----------------

Input
^^^^^
- See `DART dataset summarisation.xls <https://unioxfordnexus.sharepoint.com/:x:/r/sites/EngineeringScience-DART/Shared%20Documents/General/DART%20dataset%20summarisation.xlsx?d=w2e772ccb5717440ab47790a6b733a73b&csf=1&web=1&e=Eapex6&nav=MTJfTjNfezAwMDAwMDAwLTAwMDEtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMH0>`_

Authors and Acknowledgments
---------------------------
- OxRSE
    - John Brittain
    - Rowan Nicholls
- Kraemer Group, Department of Biology
    - Moritz Kraemer
    - Prathyush Sambaturu
- Oxford e-Research Centre, Engineering Science
    - Sarah Sparrow


# Processing Relative Wealth Index data (RWI) data processing

The python script takes as input the csv file containing relative wealth index data for locations (given by latitude and longitude) in Vietnam. It creates a geodataframe from the data in csv file and plots it on the map. The following image is the output of the script.

![rwi_plot](https://github.com/kraemer-lab/DART-Pipeline/assets/113349869/b2573e3f-2c85-4245-a2f6-d6702670aadc)
