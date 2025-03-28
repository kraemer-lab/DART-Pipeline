"""
Processing and aggregation of population-weighted Relative Wealth Index.

See the tutorial here:
https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-weigh
ted-relative-wealth-index

Originally adapted by Prathyush Sambaturu.
"""

import functools
import multiprocessing
from datetime import date
import logging

from pyquadkey2 import quadkey
from shapely.geometry import Point
import requests
import contextily
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from geoglue import Country
from bs4 import BeautifulSoup

from ..constants import OUTPUT_COLUMNS
from ..types import PartialDate, AdminLevel, URLCollection
from ..util import get_country_name, get_shapefile
from ..plots import plot_gadm_macro_heatmap
from ..paths import get_path


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
        f"{country.lower().replace(' ', '-')}-high-resolution-population-"
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
            return URLCollection("https://data.humdata.org", csvs)
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


def process_gadm_popdensity_rwi(
    iso3: str, partial_date: str = "2020", admin_level: AdminLevel = "2", plots=False
) -> pd.DataFrame:
    """
    Process population-weighted Relative Wealth Index and geospatial data.

    Purpose: Preprocess and aggregate Relative Wealth Index scores for
    administrative regions (admin2 or admin3) of Vietnam. The code for
    aggregation is adapted from the following tutorial:
    https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-
    weighted-relative-wealth-index

    Originally adapted by Prathyush Sambaturu.
    """
    logging.info("iso3:%s", iso3)
    country_name = get_country_name(iso3)
    logging.info("country_name:%s", country_name)
    pdate = PartialDate.from_string(partial_date)
    logging.info("partial_date:%s", pdate)
    year = pdate.year
    logging.info("admin_level:%s", admin_level)
    logging.info("plots:%s", plots)

    # Zoom level 14 is ~2.4km Bing tile
    zoom_level = 14

    # Import the GADM shape file
    path = get_shapefile(iso3, admin_level)
    logging.info("importing:%s", path)
    shapefile = gpd.read_file(path)
    # Get the polygons from the shape file and create a dictionary mapping the
    # region IDs to their polygon geometries
    admin_geoid = f"GID_{admin_level}"
    polygons = dict(zip(shapefile[admin_geoid], shapefile["geometry"]))

    # Import the Relative Wealth Index data
    path = get_path(
        "sources", iso3, "meta", f"{iso3.lower()}_relative_wealth_index.csv"
    )
    logging.info("importing:%s", path)
    rwi = pd.read_csv(path)
    # Assign each RWI value to an administrative region
    rwi["geo_id"] = rwi.apply(lambda x: get_geo_id(x, polygons), axis=1)
    rwi = rwi[rwi["geo_id"] != "null"]
    rwi["quadkey"] = rwi.apply(lambda x: get_quadkey(x, zoom_level), axis=1)

    # Import population density data
    path = get_path("sources", iso3, "meta", f"{iso3.lower()}_general_{year}.csv")
    logging.info("importing:%s", path)
    population = pd.read_csv(path)
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

    # Plot
    if plots:
        _, ax = plt.subplots()
        rwi.plot(
            ax=ax,
            column="rwi_weight",
            legend=True,
        )
        contextily.add_basemap(
            ax, crs="EPSG:4326", source=contextily.providers.OpenStreetMap.Mapnik
        )
        plt.title("Population-Weighted Relative Wealth Index")
        plt.xlabel("Longitude")
        plt.xticks(rotation=30)
        plt.ylabel("Latitude")
        # Export
        path = get_path(
            "output",
            iso3,
            "meta",
            f"{iso3}-{admin_level}-meta.relative_wealth_index.png",
        )
        logging.info("exporting:%s", path)
        plt.savefig(path)
        plt.close()

    # Format the output data frame
    columns = {
        "GID_0": "iso3",
        "COUNTRY": "admin_level_0",
        "NAME_1": "admin_level_1",
        "NAME_2": "admin_level_2",
        "NAME_3": "admin_level_3",
        "rwi_weight": "value",
    }
    rwi = rwi.rename(columns=columns)
    if admin_level == "0":
        rwi["admin_level_1"] = None
        rwi["admin_level_2"] = None
        rwi["admin_level_3"] = None
    elif admin_level == "1":
        rwi["admin_level_2"] = None
        rwi["admin_level_3"] = None
    elif admin_level == "2":
        rwi["admin_level_3"] = None
    rwi["year"] = year
    if pdate.month:
        rwi["month"] = pdate.month
    else:
        rwi["month"] = None
    if pdate.day:
        rwi["day"] = pdate.day
    else:
        rwi["day"] = None
    rwi["week"] = None
    rwi["metric"] = "Population-weighted relative wealth index"
    rwi["unit"] = "unitless"
    rwi["resolution"] = "~2.4 km"
    rwi["creation_date"] = date.today()
    return rwi[OUTPUT_COLUMNS]


def process_rwi_point_estimate(iso3: str) -> float:
    """Process Relative Wealth Index data only."""
    iso3 = iso3.upper()
    logging.info("iso3:%s", iso3)
    country = get_country_name(iso3)
    logging.info("country:%s", country)

    # Import the Relative Wealth Index data
    path = get_path("sources", "meta", f"{iso3.lower()}_relative_wealth_index.csv")
    logging.info("importing:%s", path)
    rwi = pd.read_csv(path)

    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(
        rwi,
        geometry=gpd.points_from_xy(rwi["longitude"], rwi["latitude"]),
        crs="EPSG:4326",  # WGS 84
    )

    return gdf.rwi.mean()


def process_gadm_rwi(iso3: str, admin_level: int, plots=False):
    """Process Relative Wealth Index and geospatial data."""
    iso3 = iso3.upper()
    logging.info("iso3:%s", iso3)
    logging.info("admin_level:%s", admin_level)
    logging.info("plots:%s", plots)

    # Create a dictionary of polygons where the key is the ID of the polygon
    # and the value is its geometry
    gdf = Country(iso3).admin(admin_level)
    admin_geoid = f"GID_{admin_level}"
    polygons = dict(zip(gdf[admin_geoid], gdf["geometry"]))

    # Import the Relative Wealth Index data
    path = get_path(
        "sources", iso3, "meta", f"{iso3.lower()}_relative_wealth_index.csv"
    )
    logging.info("importing:%s", path)
    rwi = pd.read_csv(path)

    # Create a plot
    if plots:
        data = rwi.pivot(columns="longitude", index="latitude", values="rwi")
        origin = "lower"
        min_lon = rwi["longitude"].min()
        max_lon = rwi["longitude"].max()
        min_lat = rwi["latitude"].min()
        max_lat = rwi["latitude"].max()
        extent = [min_lon, max_lon, min_lat, max_lat]
        limits = [min_lon, min_lat, max_lon, max_lat]
        zorder = 0
        country = get_country_name(iso3)
        title = f"Relative Wealth Index\n{country} - Admin Level {admin_level}"
        colourbar_label = "Relative Wealth Index [unitless]"
        path = get_path(
            "output",
            iso3,
            "meta",
            f"{iso3}-{admin_level}-meta.relative_wealth_index.png",
        )
        plot_gadm_macro_heatmap(
            data, origin, extent, limits, gdf, zorder, title, colourbar_label, path
        )

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
    rwi["iso3"] = iso3
    columns = dict(
        zip(admin_columns, [f"admin_level_{i}" for i in range(len(admin_columns))])
    )
    rwi = rwi.rename(columns=columns)
    # Add in the higher-level admin levels
    for i in range(int(admin_level) + 1, 4):
        rwi[f"admin_level_{i}"] = None
    rwi["year"] = None
    rwi["month"] = None
    rwi["day"] = None
    rwi["week"] = None
    rwi["metric"] = "meta.relative_wealth_index"
    rwi = rwi.rename(columns={"rwi": "value"})
    rwi["unit"] = "unitless"
    rwi["resolution"] = None
    rwi["creation_date"] = date.today()
    # Re-order the columns
    return rwi[OUTPUT_COLUMNS]
