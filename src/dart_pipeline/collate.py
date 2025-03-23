"""
Collate functions for DART pipeline that fetch data for the metrics.

Password management for metrics requiring authentication is done by creating a
file called `credentials.json` in the top-level of the `DART-Pipeline`
directory and adding login credentials into it in the following format:

.. code-block::

    {
        "Example metric": {
            "username": "example@email.com",
            "password": "correct horse battery staple"
        }
    }

This file is automatically ignored by Git but can be imported into scripts.

**Example Usage**

To download Daily mean temperature product (V1808) meteorological data an
`APHRODITE account <http://aphrodite.st.hirosaki-u.ac.jp/download/>`_ is
needed and the username and password need to be added to the `credentials.json`
file as described above. The script can then be run as follows (note that these
examples use the `--only-one` flags which are meant for script
testing purposes only):

.. code-block::

    # To download only one file
    uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp --only-one
    # To download all files
    uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp

This will create a `data/sources/meteorological/aphrodite-daily-mean-temp` folder
into which data will be downloaded.
"""

from .types import URLCollection
from .util import get_country_name


def gadm_data(iso3: str) -> URLCollection:
    """
    Get URLs for GADM (Database of Global Administrative Areas) data.

    See :doc:`geospatial` for more information.
    """
    return URLCollection(
        "https://geodata.ucdavis.edu/gadm/gadm4.1",
        [
            f"shp/gadm41_{iso3}_shp.zip",
            f"gpkg/gadm41_{iso3}.gpkg",
            f"json/gadm41_{iso3}_0.json",
            f"json/gadm41_{iso3}_1.json.zip",
            f"json/gadm41_{iso3}_2.json.zip",
            f"json/gadm41_{iso3}_3.json.zip",
        ],
        relative_path=iso3,
    )


def worldpop_pop_count_data(iso3: str) -> URLCollection:
    """
    WorldPop population count

    All available WorldPop datasets are detailed here:
    https://www.worldpop.org/rest/data

    This function will get population data in GeoTIFF format (as files
    with the .tif extension) along with metadata files. A zipped file (with the
    .7z extension) will also be downloaded; this will contain the same GeoTIFF
    files along with .tfw and .tif.aux.xml files. Most users will not find
    these files useful and so unzipping the .7z file is usually unnecessary.
    """
    # When iso3='VNM' the required output is country='Viet_Nam'. Hence the
    # common name ('Vietnam') is not correct
    country = get_country_name(iso3, common_name=False)
    # When country='Viet Nam', replace the space with an underscore
    country = country.replace(" ", "_")
    return URLCollection(
        "https://data.worldpop.org",
        [
            f"GIS/Population/Individual_countries/{iso3}/{country}_100m_Population/{iso3}_ppp_v2b_2020_UNadj.tif",
            # first download is tif file, which is selected when only-one flag is set,
            # otherwise zip file is downloaded
            f"GIS/Population/Individual_countries/{iso3}/{country}_100m_Population.7z",
        ],
        relative_path=iso3,
    )


def worldpop_pop_density_data(iso3: str) -> URLCollection:
    "WorldPop population density from https://www.worldpop.org/rest/data"
    year = 2020  # TODO: should this be a parameter?
    return URLCollection(
        f"https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km_UNadj/{year}/{iso3}",
        [
            # GeoDataFrame
            f"{iso3.lower()}_pd_{year}_1km_UNadj_ASCII_XYZ.zip",
            # GeoTIFF
            f"{iso3.lower()}_pd_{year}_1km_UNadj.tif",
        ],
        relative_path=iso3,
    )
