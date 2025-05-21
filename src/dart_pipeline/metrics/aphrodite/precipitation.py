"""Module for processing ."""

from datetime import date, datetime, timedelta
import logging

import numpy as np
import pandas as pd
import shapely.geometry
from geoglue.region import gadm, read_region

from ...plots import plot_gadm_scatter
from ...types import PartialDate
from ...util import days_in_year
from ...paths import get_path
from ...constants import OUTPUT_COLUMNS

# No data in APHRODITE data
# See APHRO_MA_025deg_V1901.ctl and others
NO_DATA = -99.90

logger = logging.getLogger(__name__)


def process_gadm_aphroditeprecipitation(
    iso3: str,
    admin_level: int,
    partial_date: str,
    resolution=["025deg", "050deg"],
    plots=False,
) -> pd.DataFrame:
    """
    Process GADM and APHRODITE Daily accumulated precipitation (V1901) data.

    Aggregates by given admin level for the given country (ISO3 code) and
    partial date.
    """
    pdate = PartialDate.from_string(partial_date)
    logger.info("iso3:%s", iso3)
    logger.info("admin_level:%s", admin_level)
    logger.info("partial_date:%s", pdate)
    logger.info("scope:%s", pdate.scope)
    logger.info("plots:%s", plots)

    # Import shape file
    gdf = read_region(gadm(iso3, admin_level))

    # Initialise output data frame
    output = pd.DataFrame(columns=OUTPUT_COLUMNS)  # type: ignore

    version = "V1901"
    year = pdate.year
    params = {
        # Parameters from APHRO_MA_025deg_V1901.ctl
        "025deg": {
            "n_deg": (360, 280),
            "start_coords": (60.125, -14.875),
            "scale_factor": 0.25,
        },
        # Parameters from APHRO_MA_050deg_V1901.ctl
        "050deg": {
            "n_deg": (180, 140),
            "start_coords": (60.25, -14.75),
            "scale_factor": 0.5,
        },
    }

    for res in resolution:
        nday = days_in_year(int(year))
        # Record length
        nx, ny = params[res]["n_deg"]
        recl = nx * ny
        # Longitude and latitude bounds
        x_start, y_start = params[res]["start_coords"]
        scale_factor = params[res]["scale_factor"]
        xlon = x_start + np.arange(nx) * scale_factor
        ylat = y_start + np.arange(ny) * scale_factor

        # Open the file
        file_path = get_path(
            "sources", "global", "aphrodite", f"APHRO_MA_{res}_{version}.{year}"
        )
        with open(file_path, "rb") as f:
            # Initialise arrays
            prcp_data = np.zeros((nday, ny, nx))
            rstn_data = np.zeros((nday, ny, nx))

            for iday in range(nday):
                # Read next batch of prcp values of size nx * ny
                prcp_raw = np.fromfile(f, dtype="float32", count=recl)
                prcp_raw = prcp_raw.reshape((ny, nx))
                # Read next batch of rstn values of size nx * ny
                rstn_raw = np.fromfile(f, dtype="float32", count=recl)
                rstn_raw = rstn_raw.reshape((ny, nx))
                # Store in arrays
                prcp_data[iday, :, :] = prcp_raw
                rstn_data[iday, :, :] = rstn_raw

        prcp_data = prcp_data.astype("float32")
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
                prcp_data[iday, :, :] != NO_DATA
            )
            valid_prcp = prcp_data[iday][valid_mask]
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
                valid_prcp_region = valid_prcp[region_mask]

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
                    "value": valid_prcp_region.sum(),
                    "resolution": "0.25°" if res == "025deg" else "0.5°",
                    "metric": "aphrodite-daily-precip",
                    "unit": "mm",
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

            # Scatter plot
            if plots:
                title = f"Precipitation\n{this_date}"
                colourbar_label = "Precipitation [mm]"
                path = get_path(
                    "output",
                    iso3,
                    "aphrodite",
                    f"{iso3}-{this_date}-aphrodite.total_precipitation.png",
                )
                plot_gadm_scatter(
                    valid_lon, valid_lat, valid_prcp, title, colourbar_label, path, gdf
                )

    return output
