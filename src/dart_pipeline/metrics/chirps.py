"""
CHIRPS Rainfall data
"""

import re
import logging
import datetime
from typing import Final
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rasterio.features
import rasterio.mask
import rasterio.transform
import shapely.geometry
import matplotlib.pyplot as plt

from geoglue.region import gadm

from ..constants import MIN_FLOAT
from ..plots import plot_heatmap
from ..paths import get_path
from ..util import use_range, daterange
from ..types import PartialDate, URLCollection


logger = logging.getLogger(__name__)


def chirps_rainfall_data(partial_date: str) -> list[URLCollection]:
    """
    CHIRPS Rainfall Estimates from Rain Gauge, Satellite Observations.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.

    Data is in TIF format (.tif.gz), not COG format (.cog).
    """
    pdate = PartialDate.from_string(partial_date)
    base_url = "https://data.chc.ucsb.edu"
    fmt = "tifs"  # cogs is unsupported at the moment
    chirps_first_year: Final[int] = 1981
    chirps_first_month: Final[datetime.date] = datetime.date(1981, 1, 1)
    urls: list[URLCollection] = []

    if pdate.month:
        use_range(pdate.month, 1, 12, "Month range")

    today = datetime.date.today()
    use_range(pdate.year, chirps_first_year, today.year, "CHIRPS annual data range")
    urls.append(
        URLCollection(
            f"{base_url}/products/CHIRPS-2.0/global_annual/{fmt}",
            [f"chirps-v2.0.{pdate.year}.tif"],
            relative_path="global_annual",
        )
    )

    if pdate.month:
        # Download the monthly data for the year and month provided
        month_requested = datetime.date(pdate.year, pdate.month, 1)
        this_month = datetime.date(today.year, today.month, 1)
        if chirps_first_month <= month_requested < this_month:
            base = f"{base_url}/products/CHIRPS-2.0/global_monthly/{fmt}"
            files = [f"chirps-v2.0.{pdate.year}.{pdate.month:02d}.tif.gz"]
            path = f"global_monthly/{pdate.year}"
            urls.append(URLCollection(base, files, relative_path=path))
        else:
            logger.warning(
                "Monthly data is only available from "
                + f"{chirps_first_year}-01 onwards"
            )
            return urls

        # Download the daily data for the year and month provided
        end = datetime.date(int(pdate.year), int(pdate.month) + 1, 1)
        end = end - datetime.timedelta(days=1)
        base = (
            f"{base_url}/products/CHIRPS-2.0/global_daily/" + f"{fmt}/p05/{pdate.year}"
        )
        files = [
            f"chirps-v2.0.{str(day).replace('-', '.')}.tif.gz"
            for day in daterange(month_requested, end)
        ]
        path = f"global_daily/{pdate.year}/{pdate.month:02d}"
        urls.append(URLCollection(base, files, relative_path=path))

    return urls


def get_chirps_rainfall_data_path(date: PartialDate) -> Path:
    """Get the path to a CHIRPS rainfall data file."""
    file = None
    base = "global", "chirps"
    match date.scope:
        case "daily":
            file = get_path(
                "sources",
                *base,
                "global_daily",
                str(date.year),
                date.zero_padded_month,
                f"chirps-v2.0.{date.to_string('.')}.tif",
            )
        case "monthly":
            file = get_path(
                "sources",
                *base,
                "global_monthly",
                str(date.year),
                f"chirps-v2.0.{date.to_string('.')}.tif",
            )
        case "annual":
            file = get_path(
                "sources", *base, "global_annual", f"chirps-v2.0.{date}.tif"
            )

    if not file.exists():
        raise FileNotFoundError(f"CHIRPS rainfall data not found: {file}")

    return file


def process_gadm_chirps_rainfall(
    iso3: str, admin_level: int, partial_date: str, plots=False
):
    """
    Process GADM administrative map and CHIRPS rainfall data.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.
    """
    pdate = PartialDate.from_string(partial_date)
    logger.info("iso3:%s", iso3)
    logger.info("admin_level:%s", admin_level)
    logger.info("partial_date:%s", pdate)
    logger.info("scope:%s", pdate.scope)
    logger.info("plots:%s", plots)

    # Import the GeoTIFF file
    file = get_chirps_rainfall_data_path(pdate)
    logger.info("importing:%s", file)
    src = rasterio.open(file)

    # Create a bounding box from raster bounds
    bounds = src.bounds
    raster_bbox = shapely.geometry.box(
        bounds.left, bounds.bottom, bounds.right, bounds.top
    )

    # Import shape file
    gdf = gadm(iso3, admin_level).read()
    # Transform the shape file to match the GeoTIFF's coordinate system
    gdf = gdf.to_crs(src.crs)
    # EPSG:4326 - WGS 84: latitude/longitude coordinate system based on the
    # Earth's center of mass

    # Initialise the data frame that will store the output data for each region
    columns = [
        "admin_level_0",
        "admin_level_1",
        "admin_level_2",
        "admin_level_3",
        "year",
        "month",
        "day",
        "rainfall",
    ]
    output = pd.DataFrame(columns=columns)

    # Iterate over each region in the shape file
    for i, region in gdf.iterrows():
        # Add the region name to the output data frame
        output.loc[i, "admin_level_0"] = region["COUNTRY"]
        # Initialise the graph title
        title = region["COUNTRY"]
        # Add more region names and update the graph title if the admin level
        # is high enough to warrant it
        if int(admin_level) >= 1:
            output.loc[i, "admin_level_1"] = region["NAME_1"]
            title = region["NAME_1"]
        if int(admin_level) >= 2:
            output.loc[i, "admin_level_2"] = region["NAME_2"]
            title = region["NAME_2"]
        if int(admin_level) >= 3:
            output.loc[i, "admin_level_3"] = region["NAME_3"]
            title = region["NAME_3"]

        # Add date information to the output data frame
        output.loc[i, "year"] = pdate.year
        if pdate.month:
            output.loc[i, "month"] = pdate.month
        if pdate.day:
            output.loc[i, "day"] = pdate.day

        # Check if the rainfall data intersects this region
        geometry = region.geometry
        if raster_bbox.intersects(geometry):
            # There is rainfall data for this region
            # Clip the data using the polygon of the current region. By
            # default, a pixel is included only if its center is within one of
            # the shapes
            region_data, _ = rasterio.mask.mask(src, [geometry], crop=True)
            # Replace negative values (if any exist)
            region_data = np.where(region_data < 0, np.nan, region_data)
            # Sum the pixel values to get the total for the region
            region_total = np.nansum(region_data)
        else:
            # No rainfall data for this region
            region_total = 0
        logger.info("region:%s", title)
        logger.info("region_total:%s", region_total)
        # Add the result to the output data frame
        output.loc[i, "rainfall"] = region_total

        if plots:
            # Get the bounds of the region
            min_lon, min_lat, max_lon, max_lat = geometry.bounds
            # Plot
            _, ax = plt.subplots()
            ar = region_data[0]
            ar[ar == 0] = np.nan
            im = ax.imshow(
                ar,
                cmap="coolwarm",
                origin="upper",
                extent=[min_lon, max_lon, min_lat, max_lat],
            )
            # Add the geographical borders
            gdf.plot(ax=ax, color="none", edgecolor="gray")
            gpd.GeoDataFrame([region]).plot(ax=ax, color="none", edgecolor="k")
            plt.colorbar(im, ax=ax, label="Rainfall [mm]")
            ax.set_title(f"Rainfall\n{title} - {pdate}")
            ax.set_xlim(min_lon, max_lon)
            ax.set_ylim(min_lat, max_lat)
            ax.set_ylabel("Latitude")
            ax.set_xlabel("Longitude")
            # Make the plot title file-system safe
            title = re.sub(r'[<>:"/\\|?*]', "_", title)
            title = title.strip()
            # Export
            path = get_path("output", iso3, "chirps", f"{pdate}-{title}.png")
            logger.info("exporting:%s", path)
            plt.savefig(path)
            plt.close()

    return output


def process_chirps_rainfall(partial_date: str, plots=False) -> pd.DataFrame:
    """
    Process CHIRPS Rainfall data.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.
    """
    base = "global", "chirps"
    pdate = PartialDate.from_string(partial_date)
    logger.info("partial_date:%s", pdate)
    logger.info("scope:%s", pdate.scope)
    logger.info("plots:%s", plots)

    # Import the GeoTIFF file
    file = get_chirps_rainfall_data_path(pdate)
    logger.info("importing:%s", file)
    src = rasterio.open(file)

    # Initialise the data frame that will store the output data for each region
    columns = ["year", "month", "day", "rainfall"]
    output = pd.DataFrame(columns=columns)

    # Add date information to the output data frame
    output.loc[0, "year"] = pdate.year
    if pdate.month:
        output.loc[0, "month"] = pdate.month
    if pdate.day:
        output.loc[0, "day"] = pdate.day

    # Rasterio stores image layers in 'bands'
    # Get the data in the first band as an array
    data = src.read(1)
    # Replace placeholder numbers with 0
    data[data == MIN_FLOAT] = 0
    # Hide nulls
    data[data == -9999] = 0
    # Add the result to the output data frame
    output.loc[0, "rainfall"] = np.nansum(data)

    # Create a plot
    if plots:
        title = f"Rainfall\n{pdate}"
        colourbar_label = "Rainfall [mm]"
        path = get_path("output", *base, f"{pdate}.png")
        plot_heatmap(data, title, colourbar_label, path)

    return output
