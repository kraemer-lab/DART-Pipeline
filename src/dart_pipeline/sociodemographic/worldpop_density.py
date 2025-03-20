"""Module for processing WorldPop population density."""

from datetime import date
from pathlib import Path
import logging

import numpy as np
import pandas as pd
import rasterio
import rasterio.mask

from dart_pipeline.constants import (
    OUTPUT_COLUMNS,
    BASE_DIR,
    DEFAULT_SOURCES_ROOT,
    DEFAULT_OUTPUT_ROOT,
    MIN_FLOAT,
)
from dart_pipeline.plots import plot_heatmap
from dart_pipeline.types import ProcessResult, PartialDate
from dart_pipeline.util import get_country_name


def process_worldpopdensity(iso3: str, partial_date: str, plots=False) -> ProcessResult:
    """Process WorldPop population density."""
    sub_pipeline = "sociodemographic/worldpop-density"
    logging.info("iso3:%s", iso3)
    country_name = get_country_name(iso3)
    logging.info("country_name:%s", country_name)
    pdate = PartialDate.from_string(partial_date)
    logging.info("partial_date:%s", pdate)
    logging.info("plots:%s", plots)

    # Validate inputs
    assert pdate.month is None
    assert pdate.day is None

    # Import the population density data
    iso3_lower = iso3.lower()
    path = Path(
        BASE_DIR,
        DEFAULT_SOURCES_ROOT,
        "sociodemographic",
        "worldpop-density",
        iso3,
        f"{iso3_lower}_pd_{pdate.year}_1km_UNadj.tif",
    )
    logging.info("importing:%s", path)
    src = rasterio.open(path)
    # Read data from band 1
    if src.count != 1:
        raise ValueError(f"Unexpected number of bands: {src.count}")
    source_data = src.read(1)

    # Replace placeholder numbers with 0
    # (-3.4e+38 is the smallest single-precision floating-point number)
    df = pd.DataFrame(source_data)
    data = df[df != MIN_FLOAT]
    data = data[data != -99999.0]
    value = data.mean().mean()
    logging.info("value:%s", value)

    # Create a plot
    if plots:
        data[data == 0] = np.nan
        # Take the log of the data
        log_data = np.log(data)
        title = f"Population Density\n{country_name} - {pdate.year}"
        colourbar_label = "Population Density (Log Scale)"
        path = Path(
            BASE_DIR,
            DEFAULT_OUTPUT_ROOT,
            "sociodemographic",
            "worldpop-density",
            f"{country_name} - {pdate.year}.png",
        )
        plot_heatmap(log_data, title, colourbar_label, path, extent=None, log_plot=True)

    # Initialise an output data frame
    df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Populate output data frame
    df.loc[0, "iso3"] = iso3
    df.loc[0, "admin_level_0"] = country_name
    df.loc[0, "admin_level_1"] = None
    df.loc[0, "admin_level_2"] = None
    df.loc[0, "admin_level_3"] = None
    df.loc[0, "year"] = pdate.year
    df.loc[0, "month"] = None
    df.loc[0, "day"] = None
    df.loc[0, "week"] = None
    df.loc[0, "metric"] = "Population Density"
    df.loc[0, "value"] = value
    df.loc[0, "unit"] = "people per pixel"
    df.loc[0, "resolution"] = "Admin Level 0"
    df.loc[0, "creation_date"] = date.today()

    sub_pipeline = sub_pipeline.replace("/", "_")
    filename = f"{iso3}_{sub_pipeline}_{pdate.year}_{date.today()}.csv"

    return df.fillna(""), filename
