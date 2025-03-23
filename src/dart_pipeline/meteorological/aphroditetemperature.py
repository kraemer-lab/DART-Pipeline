"""Module for processing APHRODITE temperature (V1808) data."""

from datetime import date, datetime, timedelta
from pathlib import Path
import logging
import re

import numpy as np
import pandas as pd

from ..constants import OUTPUT_COLUMNS
from ..util import days_in_year
from ..paths import get_path

# No data in APHRODITE data
# See APHRO_MA_025deg_V1901.ctl and others
NO_DATA = -99.90


def process_aphroditetemperature(year=None) -> pd.DataFrame:
    """Process APHRODITE Daily mean temperature product (V1808) data."""
    version = "V1808"
    if not year:
        # Regex pattern to match the resolution, version and year in filenames
        pattern = r"APHRO_MA_TAVE_(\d+deg)_V1808\.(\d+)"
        # Find the latest year for which there is data
        years = []
        path = get_path("sources", "global", "aphrodite")
        for filename in Path(path).iterdir():
            match = re.match(pattern, str(filename.name))
            if match:
                _, _, year = match.groups()
                years.append(int(year))
        # Get the latest year
        year = str(max(years))

    # Initialise output data frame
    output = pd.DataFrame(columns=OUTPUT_COLUMNS)

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
    for data_type in ["025deg"]:
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
        logging.info("opening:%s", path)
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
            valid_mask = (rstn_data[iday, :, :] != 0.0) & (
                temp_data[iday, :, :] != NO_DATA
            )
            valid_temp = temp_data[iday][valid_mask]

            i = len(output)
            output.loc[i, "year"] = year
            output.loc[i, "month"] = this_date.month
            output.loc[i, "day"] = this_date.day
            output.loc[i, "value"] = valid_temp.mean()
            if res == "025deg":
                output.loc[i, "resolution"] = "0.25°"
            elif res == "050deg":
                output.loc[i, "resolution"] = "0.5°"

    output["iso3"] = ""
    output["admin_level_0"] = ""
    output["admin_level_1"] = ""
    output["admin_level_2"] = ""
    output["admin_level_3"] = ""
    output["week"] = ""
    output["metric"] = "aphrodite.2m_temperature.daily_mean"
    output["unit"] = "°C"
    output["creation_date"] = date.today()

    return output
