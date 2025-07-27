"""
Processing and aggregation of population-weighted Relative Wealth Index.

See the tutorial here:
https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-weigh
ted-relative-wealth-index

Originally adapted by Prathyush Sambaturu.
"""

import functools
import multiprocessing
import logging
from typing import Final

from pyquadkey2 import quadkey
from shapely.geometry import Point
import requests
import geopandas as gpd
import pandas as pd
from bs4 import BeautifulSoup

from geoglue.region import gadm

from ..types import URLCollection
from ..util import get_country_name, iso3_admin_unpack
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
def meta_pop_density_data(iso3: str) -> URLCollection:
    """
    Download Population Density Maps from Data for Good at Meta.

    Documentation:
    https://dataforgood.facebook.com/dfg/docs/high-resolution-population-density-maps-demographic-estimates-documentation
    """
    if "-" in iso3:
        iso3, _ = iso3_admin_unpack(iso3)
    country = get_country_name(iso3)
    # Main webpage
    url = (
        "https://data.humdata.org/dataset/"
        f"{country.lower().replace(' ', '-')}-high-resolution-population-"
        "density-maps-demographic-estimates"
    )
    logger.info("Collecting links for meta.pop_density [%s]: %s", iso3, url)
    if (response := requests.get(url)).status_code == 200:
        # Search for a URL in the HTML content
        soup = BeautifulSoup(response.text, "html.parser")
        # Find all anchor tags (<a>) with href attribute containing the ISO3
        target = iso3.lower()
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
def fetch_relative_wealth_index(iso3: str) -> URLCollection:
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
    iso3, _ = iso3_admin_unpack(iso3)
    if not iso3:
        raise ValueError("No ISO3 code has been provided")

    # Search the webpage for the link(s) to the dataset(s)
    url = "https://data.humdata.org/dataset/relative-wealth-index"
    if (r := requests.get(url)).status_code == 200:
        # Search for a URL in the HTML content
        soup = BeautifulSoup(r.text, "html.parser")
        # Find all anchor tags (<a>) with href attribute containing the ISO3
        target = iso3.lower()
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
def process_gadm_popdensity_rwi(iso3: str, admin_level=2) -> pd.DataFrame:
    """
    Process population-weighted Relative Wealth Index and geospatial data.

    Purpose: Preprocess and aggregate Relative Wealth Index scores for
    administrative regions (admin2 or admin3) of Vietnam. The code for
    aggregation is adapted from the following tutorial:
    https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-
    weighted-relative-wealth-index

    Originally adapted by Prathyush Sambaturu.
    """

    # Zoom level 14 is ~2.4km Bing tile
    zoom_level: Final[int] = 14

    # Population density only available for 2020
    year: Final[int] = 2020

    shapefile = gadm(iso3, admin_level).read()
    population_path = get_path(
        "sources", iso3, "meta", "pop_density", f"{iso3.lower()}_general_2020.csv"
    )
    if not population_path.exists():
        raise FileNotFoundError(f"""Population density file not found at {population_path}
Run `uv run dart-pipeline get meta.pop_density {iso3}` to fetch data""")

    # Get the polygons from the shape file and create a dictionary mapping the
    # region IDs to their polygon geometries
    admin_geoid = f"GID_{admin_level}"
    polygons = dict(zip(shapefile[admin_geoid], shapefile["geometry"]))

    logger.info(
        "Reading meta.relative_wealth_index [%s] %r",
        iso3,
        path := get_path(
            "sources",
            iso3,
            "meta",
            "relative_wealth_index",
            f"{iso3.lower()}_relative_wealth_index.csv",
        ),
    )
    rwi = pd.read_csv(path)
    # Assign each RWI value to an administrative region
    rwi["geo_id"] = rwi.apply(lambda x: get_geo_id(x, polygons), axis=1)
    rwi = rwi[rwi["geo_id"] != "null"]
    rwi["quadkey"] = rwi.apply(lambda x: get_quadkey(x, zoom_level), axis=1)

    logger.info("Reading meta.pop_density [%s] %r", iso3, population_path)
    population = pd.read_csv(population_path)
    population = population.rename(
        columns={f"{iso3.lower()}_general_{year}": f"pop_{year}"}
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

    rwi = rwi.rename(columns={"GID_0": "ISO3", "rwi_weight": "value"}).drop(
        "geometry", axis=1
    )
    rwi["metric"] = "meta.relative_wealth_index"
    rwi["unit"] = "unitless"
    rwi["date"] = 2021
    rwi.attrs["admin"] = admin_level
    return rwi


def process_rwi_point_estimate(iso3: str) -> float:
    """Process Relative Wealth Index data only."""
    iso3 = iso3.upper()
    logger.info("iso3:%s", iso3)
    country = get_country_name(iso3)
    logger.info("country:%s", country)

    # Import the Relative Wealth Index data
    path = get_path("sources", "meta", f"{iso3.lower()}_relative_wealth_index.csv")
    logger.info("importing:%s", path)
    rwi = pd.read_csv(path)

    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(
        rwi,
        geometry=gpd.points_from_xy(rwi["longitude"], rwi["latitude"]),
        crs="EPSG:4326",  # WGS 84
    )

    return gdf.rwi.mean()


def process_gadm_rwi(iso3: str, admin_level: int):
    """Process Relative Wealth Index and geospatial data."""
    iso3 = iso3.upper()

    # Create a dictionary of polygons where the key is the ID of the polygon
    # and the value is its geometry
    gdf = gadm(iso3, admin_level).read()
    admin_geoid = f"GID_{admin_level}"
    polygons = dict(zip(gdf[admin_geoid], gdf["geometry"]))

    # Import the Relative Wealth Index data
    logger.info(
        "Reading %r",
        path := get_path(
            "sources", iso3, "meta", f"{iso3.lower()}_relative_wealth_index.csv"
        ),
    )
    rwi = pd.read_csv(path)

    # Assign each latitude and longitude to an admin region
    with multiprocessing.Pool() as p:
        geo_id = p.map(
            functools.partial(get_geo_id, polygons=polygons), rwi.to_dict("records")
        )
    rwi["geo_id"] = geo_id
    rwi = rwi[rwi["geo_id"] != "null"]

    # Get the mean RWI value for each region
    rwi = rwi.groupby("geo_id")["rwi"].mean().reset_index()

    # Dynamically choose which columns need to be added to the data
    region_columns = ["COUNTRY", "NAME_1", "NAME_2", "NAME_3"]
    admin_columns = region_columns[: int(admin_level) + 1]
    # Merge with the shapefile to get the region names
    rwi = rwi.merge(
        gdf[[admin_geoid] + admin_columns],
        left_on="geo_id",
        right_on=admin_geoid,
        how="left",
    )

    # Rename the columns
    rwi = rwi.rename(columns={"rwi": "value"})
    rwi["ISO3"] = iso3
    rwi["date"] = 2021
    rwi["metric"] = "meta.relative_wealth_index.unweighted"
    rwi["unit"] = "unitless"
    return rwi
