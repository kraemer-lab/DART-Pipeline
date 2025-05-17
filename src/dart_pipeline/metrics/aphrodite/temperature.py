"""Module for processing APHRODITE temperature (V1808) data."""

from datetime import date, datetime, timedelta
from typing import Literal
import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry

from ...util import days_in_year, get_shapefile
from ...types import PartialDate
from ...constants import OUTPUT_COLUMNS
from ...paths import get_path

logger = logging.getLogger(__name__)

# No data in APHRODITE data
# See APHRO_MA_025deg_V1901.ctl and others
NO_DATA = -99.90


def process_gadm_aphroditetemperature(
    iso3: str,
    admin_level: Literal["0", "1", "2", "3"],
    partial_date: str,
    resolution=["025deg"],
):
    """
    Process GADM and APHRODITE Daily mean temperature product (V1808) data.

    Aggregates by given admin level for the given country (ISO3 code) and
    partial date.
    """
    pdate = PartialDate.from_string(partial_date)
    logger.info("iso3:%s", iso3)
    logger.info("admin_level:%s", admin_level)
    logger.info("partial_date:%s", pdate)
    logger.info("scope:%s", pdate.scope)

    # Import shape file
    path = get_shapefile(iso3, admin_level)
    logger.info("importing:%s", path)
    gdf = gpd.read_file(path)

    # Initialise output data frame
    output = pd.DataFrame(columns=OUTPUT_COLUMNS)

    version = "V1808"
    year = pdate.year
    params = {
        # Parameters APHRO_MA_TAVE_025deg_V1808.ctl
        "025deg": {
            "product": "TAVE",
            "resolution": "025deg",
            "extension": "",
            "n_deg": (360, 280),
            "start_coords": (60.125, -14.875),
            "scale_factor": 0.25,
        },
        # Parameters APHRO_MA_TAVE_050deg_V1808.nc.ctl
        "050deg_nc": {
            "product": "TAVE",
            "resolution": "050deg",
            "extension": ".nc",
            "n_deg": (180, 140),
            "start_coords": (60.25, -14.75),
            "scale_factor": 0.5,
        },
        # Parameters APHRO_MA_TAVE_CLM_005deg_V1808.ctl
        "005deg_nc": {
            "product": "TAVE_CLM",
            "resolution": "005deg",
            "extension": ".nc",
            "n_deg": (1800, 1400),
            "start_coords": (60.025, -14.975),
            "scale_factor": 0.05,
        },
    }
    for data_type in resolution:
        nday = days_in_year(int(year))
        # Record length
        nx, ny = params[data_type]["n_deg"]
        recl = nx * ny
        # Longitude and latitude bounds
        x_start, y_start = params[data_type]["start_coords"]
        scale_factor = params[data_type]["scale_factor"]
        xlon = x_start + np.arange(nx) * scale_factor
        ylat = y_start + np.arange(ny) * scale_factor

        # Open the file
        product = params[data_type]["product"]
        res = params[data_type]["resolution"]
        ext = params[data_type]["extension"]
        path = get_path(
            "sources",
            "global",
            "aphrodite",
            f"APHRO_MA_{product}_{res}_{version}.{year}{ext}",
        )
        # Read binary data
        logger.info("importing:%s", path)
        with open(path, "rb") as f:
            # Initialise arrays
            temp_data = np.zeros((nday, ny, nx))
            rstn_data = np.zeros((nday, ny, nx))

            for iday in range(nday):
                # Read next batch of temp values of size nx * ny
                temp_raw = np.fromfile(f, dtype="float32", count=recl)
                temp_raw = temp_raw.reshape((ny, nx))
                # Read next batch of rstn values of size nx * ny
                rstn_raw = np.fromfile(f, dtype="float32", count=recl)
                rstn_raw = rstn_raw.reshape((ny, nx))
                # Store in arrays
                temp_data[iday, :, :] = temp_raw
                rstn_data[iday, :, :] = rstn_raw

        temp_data = temp_data.astype("float32")
        rstn_data = rstn_data.astype("float32")
        valid_xlon, valid_ylat = np.meshgrid(xlon, ylat, indexing="xy")

        # Iterate through days
        for iday in range(nday):
            this_date = datetime(int(year), 1, 1) + timedelta(days=iday)
            this_date = this_date.date()
            # Skip if the day doesn't match the partial date
            if pdate.month and pdate.month != this_date.month:
                continue
            if pdate.day and pdate.day != this_date.day:
                continue

            valid_mask = (rstn_data[iday, :, :] != 0.0) & (
                temp_data[iday, :, :] != NO_DATA
            )
            valid_prcp = temp_data[iday][valid_mask]
            valid_lon = valid_xlon[valid_mask]
            valid_lat = valid_ylat[valid_mask]

            # Create rows in output for each sub-region
            to_append = []
            for _, row in gdf.iterrows():
                # Extract the geometry of the current sub-region (polygon)
                region_geom = row.geometry

                # Filter to get data that falls within the sub-region geometry
                points = [
                    shapely.geometry.Point(lon, lat)
                    for lon, lat in zip(valid_lon, valid_lat)
                ]
                region_mask = np.array(
                    [region_geom.contains(point) for point in points]
                )

                # Filter data for this sub-region
                valid_temp_region = valid_prcp[region_mask]

                output_row = {
                    "iso3": iso3,
                    "admin_level_0": row["COUNTRY"],
                    "admin_level_1": row.get("NAME_1", ""),
                    "admin_level_2": row.get("NAME_2", ""),
                    "admin_level_3": row.get("NAME_3", ""),
                    "year": year,
                    "month": this_date.month,
                    "day": this_date.day,
                    "week": "",
                    "value": valid_temp_region.mean()
                    if len(valid_temp_region) > 0
                    else "",
                    "resolution": "0.25°" if res == "025deg" else "0.5°",
                    "metric": "aphrodite-daily-mean-temp",
                    "unit": "°C",
                    "creation_date": date.today(),
                }
                to_append.append(pd.DataFrame([output_row]))
            # Concatenate the new rows to the output DataFrame
            if to_append:
                # Drop all-NA columns
                to_append = [df.dropna(axis=1, how="all") for df in to_append]
                # Drop empty data frames
                to_append = [df for df in to_append if not df.empty]
                output = pd.concat([output] + to_append, ignore_index=True)

    return output
