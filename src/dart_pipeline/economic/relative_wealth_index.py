"""Module for processing Meta relative wealth index data."""

from datetime import date
from pathlib import Path
import logging
import matplotlib.pyplot as plt

import contextily as ctx
import geopandas as gpd
import pandas as pd

from dart_pipeline.constants import OUTPUT_COLUMNS, DEFAULT_OUTPUT_ROOT
from dart_pipeline.util import get_country_name, source_path


def process_rwi(iso3: str, plots=False):
    """Process Relative Wealth Index data only."""
    sub_pipeline = "economic/relative-wealth-index"
    iso3 = iso3.upper()
    logging.info("iso3:%s", iso3)
    country = get_country_name(iso3)
    logging.info("country:%s", country)
    logging.info("plots:%s", plots)

    # Import the Relative Wealth Index data
    source = "economic/relative-wealth-index"
    path = source_path(source, f"{iso3.lower()}_relative_wealth_index.csv")
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
        path = Path(DEFAULT_OUTPUT_ROOT, sub_pipeline, iso3)
        path.parent.mkdir(parents=True, exist_ok=True)
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
    # Re-order the columns
    df = df[OUTPUT_COLUMNS]

    sub_pipeline = sub_pipeline.replace("/", "_")
    filename = f"{iso3}_{sub_pipeline}_{date.today()}.csv"

    return df.fillna(""), filename
