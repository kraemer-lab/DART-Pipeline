"""
Script to collate raw data by downloading it from online sources.

See `DART dataset summarisation.xls` for information about the data fields
to be collated.

This script has been tested on Python 3.12 and more versions will be tested in
the future.

**Installation and Setup**

It is recommended to work in a virtual Python environment. Open a terminal in
the "A Collate Data" folder and run the following:

.. code-block::

    $ python3 -m venv venv
    $ source venv/bin/activate

Package requirements for this script are listed in `requirements.txt`. Install
these dependencies via:

.. code-block::

    $ python3 -m pip install -r requirements.txt

Password management is done by creating a file called `credentials.json` in the
top-level of the `DART-Pipeline` directory and adding login credentials into it
in the following format:

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
examples use the `--only_one` and `--dry_run` flags which are meant for script
testing purposes only):

.. code-block::

    # Approx run time: 4.144
    $ python3 collate_data.py "APHRODITE temperature" --only_one --dry_run
    # Approx run time: 6:36.88
    $ python3 collate_data.py "APHRODITE temperature" --only_one

This will create a `Meteorological Data` folder inside the A folder into which
data will be downloaded.
"""

import logging
import base64
import re
from datetime import date, timedelta
from typing import Final

from bs4 import BeautifulSoup
import pycountry
import requests

from .types import URLCollection, DataFile
from .util import daterange


def gadm(iso3: str) -> URLCollection:
    "Download and unpack GADM (Database of Global Administrative Areas) data"
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


def relative_wealth_index(iso3: str) -> URLCollection:
    "Relative Wealth Index"
    if not iso3:
        raise ValueError("No ISO3 code has been provided")
    if (
        r := requests.get("https://data.humdata.org/dataset/relative-wealth-index")
    ).status_code == 200:
        # Search for a URL in the HTML content
        soup = BeautifulSoup(r.text, "html.parser")
        # Find all anchor tags (<a>) with href attribute containing the ISO3
        target = iso3.lower()
        links = soup.find_all("a", href=lambda href: href and target in href)
        # Return the first link found
        if links:
            csv_url = links[0]["href"]
            return URLCollection("https://data.humdata.org", [csv_url])
        else:
            raise ValueError(f'Could not find a link containing "{target}"')
    else:
        raise ValueError(f'Bad response for page: "{r.status_code}"')


def download_ministerio_de_salud_peru_data() -> list[DataFile]:
    "Data from the Ministerio de Salud (Peru) https://www.dge.gob.pe/sala-situacional-dengue"
    pages = [
        "Nacional_dengue",
        "sala_dengue_AMAZONAS",
        "sala_dengue_ANCASH",
        "sala_dengue_AREQUIPA",
        "sala_dengue_AYACUCHO",
        "sala_dengue_CAJAMARCA",
        "sala_dengue_CALLAO",
        "sala_dengue_CUSCO",
        "sala_dengue_HUANUCO",
        "sala_dengue_ICA",
        "sala_dengue_JUNIN",
        "sala_dengue_LA LIBERTAD",
        "sala_dengue_LAMBAYEQUE",
        "sala_dengue_LIMA",
        "sala_dengue_LORETO",
        "sala_dengue_MADRE DE DIOS",
        "sala_dengue_MOQUEGUA",
        "sala_dengue_PASCO",
        "sala_dengue_PIURA",
        "sala_dengue_PUNO",
        "sala_dengue_SAN MARTIN",
        "sala_dengue_TUMBES",
        "sala_dengue_UCAYALI",
    ]
    # If the user specifies that only one dataset should be downloaded
    data: list[DataFile] = []
    for page in pages:
        url = "https://www.dge.gob.pe/sala-situacional-dengue/uploads/" + f"{page}.html"
        print(f'Accessing "{url}"')
        response = requests.get(url)
        response.raise_for_status()  # raise an exception for bad response status
        # Parse HTML content
        soup = BeautifulSoup(response.content, "html.parser")
        # Find links with the onclick attribute in both <a> and <button> tags
        onclick_elements = soup.find_all(
            lambda tag: tag.name in ["a", "button"] and tag.has_attr("onclick")
        )
        if not (links := [element.get("onclick") for element in onclick_elements]):
            raise ValueError("No links found on the page")

        for link in links:
            # Search the link for the data embedded in it
            if matches := re.findall(r"base64,(.*?)(?='\).then)", link, re.DOTALL):
                base64_string = matches[0]
            else:
                raise ValueError("No data found embedded in the link")

            # Search the link for the filename
            if matches := re.findall(r"a\.download = '(.*?)';\s*a\.click", link):
                filename = matches[0]  # there is an actual filename for this data
            else:
                filename = page + ".xlsx"  # use the page name for the file

            data.append(DataFile(filename, ".", base64.b64decode(base64_string)))
    return data


def aphrodite_precipitation_data() -> list[URLCollection]:
    "APHRODITE Daily accumulated precipitation (V1901) [requires account]"
    base_url = "http://aphrodite.st.hirosaki-u.ac.jp"
    return [
        # 0.05 degree
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/005deg",
            ["APHRO_MA_PREC_CLM_005deg_V1901.ctl.gz"],
        ),
        # 0.25 degree
        URLCollection(
            f"{base_url}/'product/APHRO_V1901/APHRO_MA/025deg",
            [
                "APHRO_MA_025deg_V1901.2015.gz",
                "APHRO_MA_025deg_V1901.ctl.gz",
            ],
        ),
        # 0.25 degree nc
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/025deg_nc",
            ["APHRO_MA_025deg_V1901.2015.nc.gz"],
        ),
        # 0.50 degree
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/050deg",
            [
                "APHRO_MA_050deg_V1901.2015.gz",
                "APHRO_MA_050deg_V1901.ctl.gz",
            ],
        ),
        # 0.50 degree nc
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/050deg_nc",
            ["APHRO_MA_050deg_V1901.2015.nc.gz"],
        ),
    ]


def aphrodite_temperature_data() -> list[URLCollection]:
    "APHRODITE Daily mean temperature product (V1808) [requires account]"

    base_url = "http://aphrodite.st.hirosaki-u.ac.jp"
    return [
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/050deg_nc",
            [
                "APHRO_MA_TAVE_050deg_V1808.2015.nc.gz",  # 19 MB
                "APHRO_MA_TAVE_050deg_V1808.nc.ctl.gz",  # 347 B
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/050deg",
            ["read_aphro_v1808.f90"],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/025deg_nc",
            [
                "APHRO_MA_TAVE_025deg_V1808.2015.nc.gz",  # 64 MB
                "APHRO_MA_TAVE_025deg_V1808.nc.ctl.gz",  # 485 B
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/025deg",
            [
                "APHRO_MA_TAVE_025deg_V1808.2015.gz",  # 64 MB
                "APHRO_MA_TAVE_025deg_V1808.ctl.gz",  # 312 B
                "read_aphro_v1808.f90",  # 2.6 KB
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/005deg_nc",
            [
                "APHRO_MA_TAVE_CLM_005deg_V1808.nc.gz",  # 1.2 GB
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/005deg",
            [
                "APHRO_MA_TAVE_CLM_005deg_V1808.ctl.gz",  # 334 B
                "APHRO_MA_TAVE_CLM_005deg_V1808.grd.gz",  # 1.4 GB
                "read_aphro_clm_v1808.f90",  # 2.1 KB
            ],
        ),
    ]


def chirps_rainfall_data(year: int, month: int | None = None) -> list[URLCollection]:
    """
    CHIRPS Rainfall Estimates from Rain Gauge, Satellite Observations.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.

    Data is in TIF format (.tif.gz), not COG format (.cog).
    """
    base_url = "https://data.chc.ucsb.edu"
    fmt = "tifs"  # cogs is unsupported at the moment
    chirps_first_year: Final[int] = 1981
    chirps_first_month: Final[date] = date(1981, 1, 1)
    urls: list[URLCollection] = []

    assert isinstance(year, int), "Year must be an integer"
    assert month is None or (
        isinstance(month, int) and 1 <= month <= 12
    ), "Month should be an integer between 1 and 12"

    today = date.today()
    if chirps_first_year <= year <= today.year:
        urls.append(
            URLCollection(
                f"{base_url}/products/CHIRPS-2.0/global_annual/{fmt}",
                [f"chirps-v2.0.{year}.tif"],
                relative_path="global_annual",
            )
        )
    else:
        logging.warning(
            f"Annual data is only available from {chirps_first_year} onwards"
        )

    if month:
        # Download the monthly data for the year and month provided
        month_requested = date(year, month, 1)
        if chirps_first_month <= month_requested < date(today.year, today.month, 1):
            urls.append(
                URLCollection(
                    f"{base_url}/products/CHIRPS-2.0/global_monthly/{fmt}",
                    [f"chirps-v2.0.{year}.{month:02d}.tif.gz"],
                    relative_path="global_monthly",
                )
            )
        else:
            logging.warning(
                f"Monthly data is only available from {chirps_first_year}-01 onwards"
            )
            return urls

        # Download the daily data for the year and month provided
        end = date(int(year), int(month) + 1, 1) - timedelta(days=1)
        urls.append(
            URLCollection(
                f"{base_url}/products/CHIRPS-2.0/global_daily/{fmt}/p05/{year}",
                [
                    f"chirps-v2.0.{str(day).replace('-', '.')}.tif.gz"
                    for day in daterange(month_requested, end)
                ],
                relative_path=f"global_daily/{year}",
            )
        )
    return urls


def terraclimate_data(year: int) -> URLCollection:
    "TerraClimate gridded temperature, precipitation, etc."
    return URLCollection(
        "https://climate.northwestknowledge.net/TERRACLIMATE-DATA",
        [
            f"TerraClimate_aet_{year}.nc",
            f"TerraClimate_def_{year}.nc",
            f"TerraClimate_PDSI_{year}.nc",  # For 2023 the capitalisation
            f"TerraClimate_pdsi_{year}.nc",  # of "PDSI" changed
            f"TerraClimate_pet_{year}.nc",
            f"TerraClimate_ppt_{year}.nc",
            f"TerraClimate_q_{year}.nc",
            f"TerraClimate_soil_{year}.nc",
            f"TerraClimate_srad_{year}.nc",
            f"TerraClimate_swe_{year}.nc",
            f"TerraClimate_tmax_{year}.nc",
            f"TerraClimate_tmin_{year}.nc",
            f"TerraClimate_vap_{year}.nc",
            f"TerraClimate_vpd_{year}.nc",
            f"TerraClimate_ws_{year}.nc",
        ],
    )


def download_meta_pop_density_data(iso3: str) -> list[URLCollection]:
    """
    Download Population Density Maps from Data for Good at Meta.

    Documentation:
    https://dataforgood.facebook.com/dfg/docs/high-resolution-population-density-maps-demographic-estimates-documentation
    """
    country = pycountry.countries.get(alpha_3=iso3).common_name
    print(f"Country:   {country}")
    # Main webpage
    url = (
        "https://data.humdata.org/dataset/"
        f'{country.lower().replace(' ', '-')}-high-resolution-population-'
        "density-maps-demographic-estimates"
    )
    if (response := requests.get(url)).status_code == 200:
        # Search for a URL in the HTML content
        soup = BeautifulSoup(response.text, "html.parser")
        # Find all anchor tags (<a>) with href attribute containing the ISO3
        target = iso3.lower()
        if links := soup.find_all("a", href=lambda href: href and target in href):
            return URLCollection(
                "https://data.humdata.org",
                [link["href"] for link in links if link["href"].endswith(".zip")],
                relative_path=iso3,
            )
        else:
            raise ValueError(f'Could not find a link containing "{target}"')
    else:
        raise ValueError(f'Bad response for page: "{response.status_code}"')


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
    country = (pycountry.countries.get(alpha_3=iso3).name).replace(" ", "_")
    return URLCollection(
        "https://data.worldpop.org",
        [
            f"GIS/Population/Individual_countries/{iso3}/{country}_100m_Population/{iso3}_ppp_v2b_2020_UNadj.tif",
            # first download is tif file, which is selected when only-one flag is set,
            # otherwise zip file is downloaded
            f"GIS/Population/Individual_countries/{iso3}/{country}_100m_Population.7z",
        ],
    )


def worldpop_pop_density_data(iso3: str) -> URLCollection:
    "WorldPop population density from https://www.worldpop.org/rest/data"
    year = 2020  # TODO: should this be a parameter?
    return URLCollection(
        f"https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km_UNadj/{year}/{iso3}",
        [
            f"{iso3.lower()}_pd_{year}_1km_UNadj_ASCII_XYZ.zip"  # GeoDataFrame
            f"{iso3.lower()}_pd_{year}_1km_UNadj.tif"  # GeoTIFF
        ],
    )


REQUIRES_AUTH = [
    "meterological/aphrodite/daily-precip",
    "meteorological/aphrodite/daily-mean-temp",
]

SOURCES = {
    "economic/relative-wealth-index": relative_wealth_index,
    "meteorological/aphrodite-daily-precip": aphrodite_precipitation_data,
    "meteorological/aphrodite-daily-mean-temp": aphrodite_temperature_data,
    "meteorological/chirps-rainfall": chirps_rainfall_data,
    "meteorological/terraclimate": terraclimate_data,
    "sociodemographic/worldpop-count": worldpop_pop_count_data,
    "sociodemographic/worldpop-density": worldpop_pop_density_data,
}