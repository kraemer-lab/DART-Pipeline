"""Utility functions for ecmwf.forecast"""

import logging
from pathlib import Path
from typing import Literal

import numpy as np
import xarray as xr
from tqdm import trange

# This is only used to fetch data from ECMWF Open Data, once downloaded and
# converted using forecast_grib_to_netcdf() the variables are renamed
# using GRIB_cfVarName
VARIABLES = [
    "2t",  # 2 meter temperature
    "2d",  # 2 meter dew point
    "sp",  # surfare pressure
    "tp",  # total precipitation
]

INSTANT_VARS = ["t2m", "d2m", "sp"]
ACCUM_VARS = ["tp"]

logger = logging.getLogger(__name__)


def cfgrib_open(
    file: str | Path,
    dataType: Literal["pf", "cf"],
    sel: dict,
    shortName: list[str] | None = None,
) -> xr.Dataset:
    filter_by_keys: dict[str, str | list[str]] = {"dataType": dataType}
    if shortName:
        filter_by_keys["shortName"] = shortName
    return xr.open_dataset(
        file,
        engine="cfgrib",
        filter_by_keys=filter_by_keys,
        decode_timedelta=True,
    ).sel(**sel)


def forecast_grib_to_netcdf(
    file: Path, sel_kwargs: dict
) -> tuple[xr.Dataset, xr.Dataset]:
    """
    This method extracts variables from the GRIB2 file, performs a spatial crop
    (open data is downloaded for the whole world) and saves them in a netcdf file.

    This method extracts data from the GRIB2 file in stages, both to keep memory
    usage low and due to GRIB2 files not allowing simultaneous extraction of variables
    in different levels (such as surface pressure and temperature). We also need to
    perform separate extractions for the control and perturbed simulations.

    Parameters
    ----------
    file : Path
        Path of the GRIB2 file to read
    sel_kwargs : dict
        Dictionary with the keywords to select the data, such as lat and lon, for
        example, for the region of Vietnam it would be
        ```python
        sel_kwargs = {"latitude": slice(24, 8), "longitude": slice(102, 110)}
        ```
    output_folder : Path
        Folder to write netCDF files in (default=None). If not specified,
        writes to standard DART location:
        ``~/.local/share/dart-pipeline/sources/VNM/ecmwf/``
    output_stem : str
        Stem of the output file, usually comprising the iso3 code
        and date, e.g. VNM-2025-05-16

    Returns
    -------
    tuple[xr.Dataset, xr.Dataset]
        Tuple of (instant, accum) xarray Datasets converted from GRIB2 file
    """
    logger.info("Converting from GRIB2 to netCDF: %s", file)
    pf = cfgrib_open(file, "pf", sel_kwargs)
    pf_temp = cfgrib_open(file, "pf", sel_kwargs, ["2t", "2d"])
    n_ensembles = len(pf.number)
    # Number of simulations processed at once. Trying to process all the data at
    # once causes the computer to crash so this function determines how many
    # simulations do we process at once
    sims = np.arange(1, n_ensembles, 5)

    if n_ensembles not in sims:
        sims = np.append(sims, n_ensembles)

    # The following opens the grid data, loads all the variable and simulations
    # and transforms it into a single xarray that is then saved as a netcdf.
    # This is because xarray does not let load data from different simulations
    # (control and perturbed forecasts) or variables that are in different levels
    # (such as 2 meter temperature and surface pressure). Hence we need to load
    # the datasets separately and then join them together into a single xarray.

    # Read control forecast as ensemble index 0
    inter_tot = xr.merge(
        [
            cfgrib_open(file, "cf", sel_kwargs),
            cfgrib_open(file, "cf", sel_kwargs, ["2t", "2d"]),
        ],
        compat="override",
    )
    for i in trange(len(sims) - 1, desc="Converting simulation groups"):
        number_slice = (
            slice(sims[0], sims[1]) if i == 0 else slice(sims[i] + 1, sims[i + 1])
        )
        pf = xr.merge(
            [pf.sel(number=number_slice), pf_temp.sel(number=number_slice)],
            compat="override",
        )
        inter_tot = xr.concat([inter_tot, pf], dim="number")
    return inter_tot[INSTANT_VARS], inter_tot[ACCUM_VARS]
