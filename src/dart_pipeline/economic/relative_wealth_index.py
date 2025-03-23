"""Module for processing Meta relative wealth index data."""

from datetime import date
import logging
import matplotlib.pyplot as plt

import contextily as ctx
import geopandas as gpd
import pandas as pd

from ..constants import OUTPUT_COLUMNS
from ..util import get_country_name
from ..paths import get_path


def process_rwi(iso3: str, plots=False):
    """Process Relative Wealth Index data only."""
    sub_pipeline = "meta", "relative_wealth_index"
    iso3 = iso3.upper()
    logging.info("iso3:%s", iso3)
    country = get_country_name(iso3)
    logging.info("country:%s", country)
    logging.info("plots:%s", plots)

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

    # Create a plot
    if plots:
        _, ax = plt.subplots()
        # Plot the data using EPSG:4326 (WGS 84) projection
        gdf.plot(ax=ax, column="rwi", cmap="coolwarm", markersize=5, legend=True)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        # Add a basemap using EPSG:4326 (WGS 84) projection
        ctx.add_basemap(ax, crs="EPSG:4326", source=ctx.providers.OpenStreetMap.Mapnik)
        # Export
        path = get_path("output", iso3, *sub_pipeline)
        logging.info("exporting:%s", path)
        plt.savefig(path)
        plt.close()

    # Create an output data frame
    df = pd.DataFrame(
        {
            "iso3": [iso3],
            "admin_level_0": [country],
            "admin_level_1": [None],
            "admin_level_2": [None],
            "admin_level_3": [None],
            "year": [None],
            "month": [None],
            "day": [None],
            "week": [None],
            "metric": ["Relative Wealth Index"],
            "value": [gdf["rwi"].mean()],
            "unit": ["unitless"],
            "resolution": [None],
            "creation_date": [date.today()],
        }
    )
    return df[OUTPUT_COLUMNS]
