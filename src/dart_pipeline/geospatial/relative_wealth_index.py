"""Module for processing Meta relative wealth index data."""

from datetime import date
import logging

import pandas as pd
import shapely.geometry
from geoglue import Country
from pandarallel import pandarallel

from ..constants import OUTPUT_COLUMNS
from ..plots import plot_gadm_macro_heatmap
from ..util import get_country_name
from ..paths import get_path

pandarallel.initialize(progress_bar=True)


def get_admin_region(lat: float, lon: float, polygons) -> str:
    """
    Find the admin region in which a grid cell lies.

    Return the ID of administrative region in which the centre (given by
    latitude and longitude) of a 2.4km^2 grid cell lies.
    """
    point = shapely.geometry.Point(lon, lat)
    for geo_id in polygons:
        polygon = polygons[geo_id]
        if polygon.contains(point):
            return geo_id
    return "null"


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

    def get_admin(x):
        return get_admin_region(x["latitude"], x["longitude"], polygons)

    # Assign each latitude and longitude to an admin region
    rwi["geo_id"] = rwi.parallel_apply(get_admin, axis=1)  # type: ignore
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
