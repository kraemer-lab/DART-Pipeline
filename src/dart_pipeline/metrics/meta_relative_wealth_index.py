"""
Processing and aggregation of population-weighted Relative Wealth Index.

See the tutorial here:
https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-weigh
ted-relative-wealth-index

Originally adapted by Prathyush Sambaturu.
"""

import logging
from typing import Final

import xarray as xr
from pyquadkey2 import quadkey
from shapely.geometry import Point
import requests
import pandas as pd
from bs4 import BeautifulSoup

from geoglue.region import BaseCountry, CountryAdministrativeLevel

from ..types import URLCollection
from ..util import get_country_name
from ..paths import get_path
from ..metrics import register_metrics, register_fetch, register_process

logger = logging.getLogger(__name__)

register_metrics(
    "meta",
    description="Sociodemographic indicators from Meta",
    metrics={
        "pop_density": {
            "url": "https://dataforgood.facebook.com/dfg/docs/high-resolution-population-density-maps-demographic-estimates-documentation",
            "long_name": "Meta population density",
            "units": "1",
            "citation": """
            Facebook Connectivity Lab and Center for International Earth Science
            Information Network - CIESIN – Columbia University. 2016.
            High Resolution Settlement Layer (HRSL). Source imagery for
            HRSL © 2016 DigitalGlobe. Accessed YYYY-MM-DD""",
        },
        "relative_wealth_index": {
            "url": "https://dataforgood.facebook.com/dfg/tools/relative-wealth-index",
            "long_name": "Relative wealth index",
            "units": "1",
            "license": "CC-BY-NC-4.0",
            "depends": ["pop_density"],
            "citation": """
                Microestimates of wealth for all low- and middle-income countries.
                Guanghua Chi, Han Fang, Sourav Chatterjee, Joshua E. Blumenstock.
                Proceedings of the National Academy of Sciences Jan 2022, 119 (3)
                e2113658119; DOI: 10.1073/pnas.2113658119""",
        },
    },
)


@register_fetch("meta.pop_density")
def meta_pop_density_data(region: BaseCountry) -> URLCollection:
    """
    Download Population Density Maps from Data for Good at Meta.

    Documentation:
    https://dataforgood.facebook.com/dfg/docs/high-resolution-population-density-maps-demographic-estimates-documentation
    """
    assert region.iso3 is not None
    country_name = get_country_name(region.iso3)
    # Main webpage
    url = (
        "https://data.humdata.org/dataset/"
        f"{country_name.lower().replace(' ', '-')}-high-resolution-population-"
        "density-maps-demographic-estimates"
    )
    logger.info("Collecting links for meta.pop_density [%s]: %s", region.iso3, url)
    if (response := requests.get(url)).status_code == 200:
        # Search for a URL in the HTML content
        soup = BeautifulSoup(response.text, "html.parser")
        # Find all anchor tags (<a>) with href attribute containing the ISO3
        target = region.iso3.lower()
        if links := soup.find_all("a", href=lambda href: href and target in href):  # type: ignore
            return URLCollection(
                "https://data.humdata.org",
                [
                    link["href"]
                    for link in links
                    if link["href"].endswith(".zip")
                    and "general" in link["href"]
                    and "csv" in link["href"]
                ],
            )
        else:
            raise ValueError(f'Could not find a link containing "{target}"')
    else:
        raise ValueError(f'Bad response for page: "{response.status_code}"')


@register_fetch("meta.relative_wealth_index")
def fetch_relative_wealth_index(region: BaseCountry) -> URLCollection:
    """This dataset contains the relative wealth index, which is the relative
    standard of living, obtained from connectivity data, satellite imagery and
    other sources. Cite the following if using this dataset:

        Microestimates of wealth for all low- and middle-income countries.
        Guanghua Chi, Han Fang, Sourav Chatterjee, Joshua E. Blumenstock
        Proceedings of the National Academy of Sciences
        Jan 2022, 119 (3) e2113658119; DOI: 10.1073/pnas.2113658119

    Upstream URL: https://data.humdata.org/dataset/relative-wealth-index
    """
    # Validate input parameter
    assert region.iso3 is not None

    # Search the webpage for the link(s) to the dataset(s)
    url = "https://data.humdata.org/dataset/relative-wealth-index"
    if (r := requests.get(url)).status_code == 200:
        # Search for a URL in the HTML content
        soup = BeautifulSoup(r.text, "html.parser")
        # Find all anchor tags (<a>) with href attribute containing the ISO3
        target = region.iso3.lower()
        links = soup.find_all("a", href=lambda href: href and target in href)  # type: ignore
        # Return the first link found
        if links:
            csvs = [link["href"] for link in links if "csv" in link["href"]]
            return URLCollection("https://data.humdata.org", csvs, unpack=False)
        else:
            raise ValueError(f'Could not find a link containing "{target}"')
    else:
        raise ValueError(f'Bad response for page: "{r.status_code}"')


def get_geo_id(x: dict[str, float], polygons: dict) -> str:
    """
    Find the administrative region ID containing a given point.

    Args:
        lat (float): Latitude of the point.
        lon (float): Longitude of the point.
        polygons (dict): Dictionary mapping region IDs to their polygon
            geometries.

    Returns:
        str: The ID of the region containing the point, or 'null' if not found.
    """
    point = Point(x["longitude"], x["latitude"])
    for geo_id in polygons:
        polygon = polygons[geo_id]
        if polygon.contains(point):
            return geo_id
    return "null"


def get_quadkey(x, zoom_level):
    """Get the quadkey for a latitude and longitude at a zoom level."""
    return str(quadkey.from_geo((x["latitude"], x["longitude"]), zoom_level))


def get_admin_region(lat: float, lon: float, polygons) -> str:
    """
    Find the admin region in which a grid cell lies.

    Return the ID of administrative region in which the centre (given by
    latitude and longitude) of a 2.4km^2 grid cell lies.
    """
    point = Point(lon, lat)
    for geo_id in polygons:
        polygon = polygons[geo_id]
        if polygon.contains(point):
            return geo_id
    return "null"


@register_process("meta.relative_wealth_index")
def process_popdensity_rwi(region: CountryAdministrativeLevel) -> xr.DataArray:
    """
    Process population-weighted Relative Wealth Index and geospatial data.

    Purpose: Preprocess and aggregate Relative Wealth Index scores for
    administrative regions (admin2 or admin3) of Vietnam. The code for
    aggregation is adapted from the following tutorial:
    https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-
    weighted-relative-wealth-index

    Originally adapted by Prathyush Sambaturu.
    """
    assert region.iso3 is not None
    # Zoom level 14 is ~2.4km Bing tile
    zoom_level: Final[int] = 14

    # Population density only available for 2020
    year: Final[int] = 2020

    shapefile = region.read()
    population_path = get_path(
        "sources",
        region.name,
        "meta",
        "pop_density",
        f"{region.name.lower()}_general_2020.csv",
    )
    print("POPULATION PATH", population_path)
    if not population_path.exists():
        raise FileNotFoundError(f"""Population density file not found at {population_path}
Run `uv run dart-pipeline get meta.pop_density {region.iso3}` to fetch data""")

    # Get the polygons from the shape file and create a dictionary mapping the
    # region IDs to their polygon geometries
    admin_geoid = region.pk
    polygons = dict(zip(shapefile[admin_geoid], shapefile["geometry"]))

    logger.info(
        "Reading meta.relative_wealth_index [%s] %r",
        region.name,
        path := get_path(
            "sources",
            region.name,
            "meta",
            "relative_wealth_index",
            f"{region.name.lower()}_relative_wealth_index.csv",
        ),
    )
    print("RWI PATH", path)
    rwi = pd.read_csv(path)
    # Assign each RWI value to an administrative region
    rwi["geo_id"] = rwi.apply(get_geo_id, axis=1, args=(polygons,))
    rwi = rwi[rwi["geo_id"] != "null"]
    rwi["quadkey"] = rwi.apply(get_quadkey, axis=1, args=(zoom_level,))

    logger.info("Reading meta.pop_density [%s] %r", region.name, population_path)
    population = pd.read_csv(population_path)
    population = population.rename(
        columns={f"{region.name.lower()}_general_{year}": f"pop_{year}"}
    )
    # Aggregates the data by Bing tiles at zoom level 14
    population["quadkey"] = population.apply(
        lambda x: get_quadkey(x, zoom_level), axis=1
    )
    population = population.groupby("quadkey", as_index=False)[f"pop_{year}"].sum()

    # Merge the RWI and population density data
    rwi_pop = rwi.merge(population, on="quadkey", how="inner")
    geo_pop = rwi_pop.groupby("geo_id", as_index=False)[f"pop_{year}"].sum()
    geo_pop = geo_pop.rename(columns={f"pop_{year}": f"geo_{year}"})
    rwi_pop = rwi_pop.merge(geo_pop, on="geo_id", how="inner")
    rwi_pop["pop_weight"] = rwi_pop[f"pop_{year}"] / rwi_pop[f"geo_{year}"]
    rwi_pop["rwi_weight"] = rwi_pop["rwi"] * rwi_pop["pop_weight"]
    geo_rwi = rwi_pop.groupby("geo_id", as_index=False)["rwi_weight"].sum()

    # Merge the population-weight RWI data with the GADM shapefile
    rwi = shapefile.merge(geo_rwi, left_on=admin_geoid, right_on="geo_id")

    rwi = rwi.rename(
        columns={"GID_0": "ISO3", "rwi_weight": "value", region.pk: "region"}
    ).drop("geometry", axis=1)
    series = (
        rwi[["region", "value"]]
        .set_index("region")
        .value.rename("rwi")
        .astype("float32")
    )
    rwi_a = xr.DataArray(series)
    rwi_a.attrs.update(
        {"DART_region": str(region), "long_name": "Relative wealth index", "units": "1"}
    )
    return rwi_a.expand_dims(time=[pd.Timestamp("2021")])
