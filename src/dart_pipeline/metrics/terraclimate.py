import logging
import platform
import datetime

import rasterio
import numpy as np
import pandas as pd
import netCDF4 as nc
from geoglue.region import gadm

from ..types import PartialDate, URLCollection
from ..util import use_range
from ..paths import get_path

logger = logging.getLogger(__name__)

TERRACLIMATE_METRICS = [
    "aet",  # water_evaporation_amount_mm
    "def",  # water_potential_evaporation_amount_minus_water_evaporatio
    "pdsi",  # palmer_drought_severity_index (unitless)
    "pet",  # water_potential_evaporation_amount_mm
    "ppt",  # precipitation_amount_mm
    "q",  # runoff_amount_mm
    "soil",  # soil_moisture_content_mm
    "srad",  # downwelling_shortwave_flux_in_air_W_per_m_squared
    "swe",  # liquid_water_content_of_surface_snow_mm
    "tmax",  # air_temperature_max_degC
    "tmin",  # air_temperature_min_degC
    "vap",  # water_vapor_partial_pressure_in_air_kPa
    "vpd",  # vapor_pressure_deficit_kPa
    "ws",  # wind_speed_m_per_s
]


def terraclimate_data(year: int) -> URLCollection:
    """TerraClimate gridded temperature, precipitation data.

    TerraClimate is a dataset of monthly climate and climatic water balance for
    terrestrial surfaces from 1958--2023. Data have a monthly temporal
    resolution and 4 km (1/24th degree) spatial resolution.

    Upstream URL: https://www.climatologylab.org/terraclimate.html
    """
    use_range(year, 1958, 2023, "Terraclimate year range")
    return URLCollection(
        "https://climate.northwestknowledge.net/TERRACLIMATE-DATA",
        # 2023, capitalisation of PDSI changed
        [f"TerraClimate_PDSI_{year}.nc"]
        + [f"TerraClimate_{metric}_{year}.nc" for metric in TERRACLIMATE_METRICS],
    )


def process_terraclimate(partial_date: str, iso3: str, admin_level: str):
    """
    Process TerraClimate data.

    This metric incorporates TerraClimate gridded temperature, precipitation,
    and other water balance variables. The data is stored in NetCDF (`.nc`)
    files for which the `netCDF4` library is needed.
    """
    pdate = PartialDate.from_string(partial_date)
    logger.info("partial_date:%s", pdate)
    iso3 = iso3.upper()
    logger.info("iso3:%s", iso3)
    logger.info("admin_level:%s", admin_level)

    # Initialise output data frame
    columns = [
        "admin_level_0",
        "admin_level_1",
        "admin_level_2",
        "admin_level_3",
        "year",
        "month",
    ]
    output = pd.DataFrame(columns=columns)

    gdf = gadm(iso3, admin_level).read()

    # Iterate over the metrics
    for metric in TERRACLIMATE_METRICS:
        # Import the raw data
        if (pdate.year == 2023) and (metric == "pdsi"):
            # In 2023 the capitalization of pdsi changed
            # The capitalisation of PDSI changes depending on how your OS
            # handles case sensitivity
            if platform.system() == "Linux":
                filename = f"TerraClimate_PDSI_{pdate.year}.nc"
                metric = "PDSI"
            elif platform.system() == "Darwin":
                filename = f"TerraClimate_pdsi_{pdate.year}.nc"
                metric = "pdsi"
        else:
            filename = f"TerraClimate_{metric}_{pdate.year}.nc"
        path = get_path("sources", "global", "terraclimate", filename)
        logger.info("importing:%s", path)
        ds = nc.Dataset(path)

        # Extract the variables
        lat = ds.variables["lat"][:]
        lon = ds.variables["lon"][:]
        time = ds.variables["time"][:]  # Time in days since 1900-01-01
        raw = ds.variables[metric]

        # Check if a standard name is provided for this metric
        try:
            standard_name = raw.standard_name
        except AttributeError:
            standard_name = metric

        # Apply scale factor
        data = raw[:].astype(np.float32)
        data = data * raw.scale_factor + raw.add_offset
        # Replace fill values with NaN
        data[data == raw._FillValue] = np.nan

        # Convert time to actual dates
        base_time = datetime.datetime(1900, 1, 1)
        months = [base_time + datetime.timedelta(days=t) for t in time]

        for i, month in enumerate(months):
            # If a month has been specified on the command line
            if pdate.month:
                # If this data come from a month that does not match the
                # requested month
                if pdate.month != month.month:
                    # Skip this iteration
                    continue

            # Extract the data for the chosen month
            this_month = data[i, :, :]

            # Iterate over the regions in the shape file
            for j, region in gdf.iterrows():
                geometry = region.geometry

                # Initialise a new row for the output data frame
                idx = i * len(months) + j
                output.loc[idx, "admin_level_0"] = region["COUNTRY"]
                output.loc[idx, "admin_level_1"] = None
                output.loc[idx, "admin_level_2"] = None
                output.loc[idx, "admin_level_3"] = None
                output.loc[idx, "year"] = month.year
                output.loc[idx, "month"] = month.month
                # Initialise the graph title
                # Update the new row and the title if the admin level is high
                # enough
                if int(admin_level) >= 1:
                    output.loc[idx, "admin_level_1"] = region["NAME_1"]
                if int(admin_level) >= 2:
                    output.loc[idx, "admin_level_2"] = region["NAME_2"]
                if int(admin_level) >= 3:
                    output.loc[idx, "admin_level_3"] = region["NAME_3"]

                # Define transform for geometry_mask based on grid resolution
                transform = rasterio.transform.from_origin(
                    lon.min(), lat.max(), abs(lon[1] - lon[0]), abs(lat[1] - lat[0])
                )

                # Create a mask that is True for points outside the geometries
                mask = rasterio.features.geometry_mask(
                    [geometry], transform=transform, out_shape=this_month.shape
                )
                masked_data = np.ma.masked_array(this_month, mask=mask)

                # Add to output data frame
                output.loc[idx, standard_name] = np.nansum(masked_data)

        # Close the NetCDF file after use
        ds.close()

    return output
