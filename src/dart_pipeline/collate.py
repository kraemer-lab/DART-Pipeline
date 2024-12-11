"""
Script to collate raw data by downloading it from online sources.

See `DART dataset summarisation.xls` for information about the data fields
to be collated.

This script has been tested on Python 3.12 and more versions will be tested in
the future. See README.md for installation instructions.

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
examples use the `--only-one` flags which are meant for script
testing purposes only):

.. code-block::

    # To download only one file
    $ uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp --only-one
    # To download all files
    $ uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp

This will create a `data/sources/meteorological/aphrodite-daily-mean-temp` folder
into which data will be downloaded.
"""

import logging
import base64
import re
from datetime import date, timedelta
from typing import Final, Callable

from bs4 import BeautifulSoup
import requests

from .collate_api import download_era5_reanalysis_data
from .constants import TERRACLIMATE_METRICS, PERU_REGIONS
from .types import URLCollection, DataFile, PartialDate
from .util import daterange, use_range, get_country_name


def gadm_data(iso3: str, unpack: bool = False) -> URLCollection:
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
    url = "https://data.humdata.org/dataset/relative-wealth-index"
    if (r := requests.get(url)).status_code == 200:
        # Search for a URL in the HTML content
        soup = BeautifulSoup(r.text, "html.parser")
        # Find all anchor tags (<a>) with href attribute containing the ISO3
        target = iso3.lower()
        links = soup.find_all("a", href=lambda href: href and target in href)  # type: ignore
        # Return the first link found
        if links:
            csv_url = links[0]["href"]
            return URLCollection("https://data.humdata.org", [csv_url])
        else:
            raise ValueError(f'Could not find a link containing "{target}"')
    else:
        raise ValueError(f'Bad response for page: "{r.status_code}"')


def ministerio_de_salud_peru_data() -> list[DataFile]:
    """
    Download data from the Ministerio de Salud (Peru).

    https://www.dge.gob.pe/sala-situacional-dengue
    """
    pages = ["Nacional_dengue"] + \
        ["sala_dengue_" + region for region in PERU_REGIONS]
    # If the user specifies that only one dataset should be downloaded
    data: list[DataFile] = []
    for page in pages:
        url = "https://www.dge.gob.pe/sala-situacional-dengue/uploads/" + \
            f"{page}.html"
        print(f'Accessing {url}')
        response = requests.get(url)
        # Raise an exception for bad response status
        response.raise_for_status()
        # Parse HTML content
        soup = BeautifulSoup(response.content, "html.parser")
        # Find links with the onclick attribute in both <a> and <button> tags
        onclick_elements = soup.find_all(
            lambda tag: tag.name in ["a", "button"] and tag.has_attr("onclick")
        )
        links = [element.get("onclick") for element in onclick_elements]
        if not links:
            raise ValueError("No links found on the page")

        for link in links:
            # Search the link for the data embedded in it
            matches = re.findall(r"base64,(.*?)(?='\).then)", link, re.DOTALL)
            if matches:
                base64_string = matches[0]
            else:
                raise ValueError("No data found embedded in the link")

            # Search the link for the filename
            matches = re.findall(r"a\.download = '(.*?)';\s*a\.click", link)
            if matches:
                # There is an actual filename for this data
                filename = matches[0]
            else:
                # Use the page name for the file
                filename = page + ".xlsx"

            file = DataFile(filename, ".", base64.b64decode(base64_string))
            data.append(file)
    return data


def aphrodite_precipitation_data(unpack) -> list[URLCollection]:
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
            f"{base_url}/product/APHRO_V1901/APHRO_MA/025deg",
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


def aphrodite_temperature_data(unpack) -> list[URLCollection]:
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


def chirps_rainfall_data(partial_date: str) -> list[URLCollection]:
    """
    CHIRPS Rainfall Estimates from Rain Gauge, Satellite Observations.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.

    Data is in TIF format (.tif.gz), not COG format (.cog).
    """
    pdate = PartialDate.from_string(partial_date)
    base_url = 'https://data.chc.ucsb.edu'
    fmt = 'tifs'  # cogs is unsupported at the moment
    chirps_first_year: Final[int] = 1981
    chirps_first_month: Final[date] = date(1981, 1, 1)
    urls: list[URLCollection] = []
    if pdate.month:
        use_range(pdate.month, 1, 12, 'Month range')

    today = date.today()
    use_range(
        pdate.year, chirps_first_year, today.year, 'CHIRPS annual data range'
    )
    urls.append(
        URLCollection(
            f"{base_url}/products/CHIRPS-2.0/global_annual/{fmt}",
            [f"chirps-v2.0.{pdate.year}.tif"],
            relative_path="global_annual",
        )
    )

    if pdate.month:
        # Download the monthly data for the year and month provided
        month_requested = date(pdate.year, pdate.month, 1)
        this_month = date(today.year, today.month, 1)
        if chirps_first_month <= month_requested < this_month:
            base = f'{base_url}/products/CHIRPS-2.0/global_monthly/{fmt}'
            files = [f'chirps-v2.0.{pdate.year}.{pdate.month:02d}.tif.gz']
            path = f'global_monthly/{pdate.year}'
            urls.append(URLCollection(base, files, relative_path=path))
        else:
            logging.warning(
                'Monthly data is only available from ' +
                f'{chirps_first_year}-01 onwards'
            )
            return urls

        # Download the daily data for the year and month provided
        end = date(int(pdate.year), int(pdate.month) + 1, 1)
        end = end - timedelta(days=1)
        base = f'{base_url}/products/CHIRPS-2.0/global_daily/' + \
            f'{fmt}/p05/{pdate.year}'
        files = [
            f"chirps-v2.0.{str(day).replace('-', '.')}.tif.gz"
            for day in daterange(month_requested, end)
        ]
        path = f'global_daily/{pdate.year}/{pdate.month:02d}'
        urls.append(URLCollection(base, files, relative_path=path))

    return urls


def terraclimate_data(year: int) -> URLCollection:
    "TerraClimate gridded temperature, precipitation, etc."
    use_range(year, 1958, 2023, "Terraclimate year range")
    return URLCollection(
        "https://climate.northwestknowledge.net/TERRACLIMATE-DATA",
        # 2023, capitalisation of PDSI changed
        [f"TerraClimate_PDSI_{year}.nc"]
        + [f"TerraClimate_{metric}_{year}.nc" for metric in TERRACLIMATE_METRICS],
    )


def meta_pop_density_data(iso3: str) -> URLCollection:
    """
    Download Population Density Maps from Data for Good at Meta.

    Documentation:
    https://dataforgood.facebook.com/dfg/docs/high-resolution-population-density-maps-demographic-estimates-documentation
    """
    country = get_country_name(iso3)
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
        if links := soup.find_all("a", href=lambda href: href and target in href):  # type: ignore
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
    # When iso3='VNM' the required output is country='Viet_Nam'. Hence the
    # common name ('Vietnam') is not correct
    country = get_country_name(iso3, common_name=False)
    # When country='Viet Nam', replace the space with an underscore
    country = country.replace(' ', '_')
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
            f"{iso3.lower()}_pd_{year}_1km_UNadj.tif"
        ],
        relative_path=iso3,
    )


REQUIRES_AUTH = [
    "meteorological/aphrodite-daily-precip",
    "meteorological/aphrodite-daily-mean-temp",
]

SOURCES: dict[
    str, Callable[..., URLCollection | list[URLCollection] | list[DataFile]]
] = {
    'economic/relative-wealth-index': relative_wealth_index,
    'epidemiological/dengue/peru': ministerio_de_salud_peru_data,
    'geospatial/gadm': gadm_data,
    'meteorological/aphrodite-daily-mean-temp': aphrodite_temperature_data,
    'meteorological/aphrodite-daily-precip': aphrodite_precipitation_data,
    'meteorological/chirps-rainfall': chirps_rainfall_data,
    'meteorological/era5-reanalysis': download_era5_reanalysis_data,
    'meteorological/terraclimate': terraclimate_data,
    'sociodemographic/meta-pop-density': meta_pop_density_data,
    'sociodemographic/worldpop-count': worldpop_pop_count_data,
    'sociodemographic/worldpop-density': worldpop_pop_density_data,
}
