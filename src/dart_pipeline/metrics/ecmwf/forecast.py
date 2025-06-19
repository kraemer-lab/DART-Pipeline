"""Utility functions for ecmwf.forecast"""

import logging
import functools
import multiprocessing
from pathlib import Path
from typing import Literal

import numpy as np
import xarray as xr
import geoglue.zonal_stats
from tqdm import trange
from geoglue import MemoryRaster
from geoglue.types import Bbox
from geoglue.region import Region
from geoglue.resample import resampled_dataset

from ...metrics.worldpop import get_worldpop

logger = logging.getLogger(__name__)

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


def zonal_stats(
    var: str,
    ds: xr.Dataset,
    region: Region,
    weights: MemoryRaster,
    sims: int | None = None,
) -> xr.DataArray:
    """Return zonal statistics for a particular DataArray as another xarray DataArray

    This is a wrapper around geoglue.zonal_stats_xarray to add CF-compliant
    metadata attributes derived from the metric name

    Parameters
    ----------
    var : str
        Variable in xarray.Dataset to perform zonal statistics on
    da : xr.Dataset
        xarray Dataset to perform zonal statistics on. Must have
        'latitude', 'longitude' and a time coordinate
    region : geoglue.region.Region
        Region for which to calculate zonal statistics
    weights : MemoryRaster
        Uses the specified raster to perform weighted zonal statistics
    sims : int
        Number of simulations to perform zonal statistics for, default=None which
        selects all simulations

    Returns
    -------
    xr.DataArray
        The geometry ID is specified in the ``region`` coordinate. CF-compliant
        metadata attributes are attached:

        - ``standard_name``: CF standard name, if present
        - ``long_name``: Description of the metric
        - ``units``: CF-compliant units according to the udunits2 package
        - ``cell_methods``: Aggregation methods used to derive values in DataArray,
           e.g. ``time: sum (interval: 1 week)`` to indicate a weekly summation.

    """
    da = ds[var]
    geom = region.read()
    operation = (
        "mean(coverage_weight=area_spherical_km2)"
        if da.name not in ["tp", "tp_bc"]
        else "area_weighted_sum"
    )
    n_sims: int = da.number.size
    sims = sims or n_sims
    za = xr.concat(
        [
            geoglue.zonal_stats.zonal_stats_xarray(
                da.sel(number=i), geom, operation, weights, region_col=region.pk
            ).rename(da.name)
            for i in range(sims)
        ],
        dim="number",
    )
    za = za.assign_coords(number=range(len(za.number)))
    x, y, z = za.shape  # (time, region, number)
    call = f"zonal_stats(da, {region.name}, {operation=}, {weights=}"
    if x * y * z == 0:
        raise ValueError(f"Zero dimension DataArray created from {call}", call)
    za.attrs = da.attrs.copy()
    za.attrs["DART_region"] = str(region)
    za.attrs["DART_zonal_stats"] = call
    return za


def forecast_zonal_stats(
    ds: xr.Dataset, region: Region, pop_year: int, sims: int | None = None
) -> xr.Dataset:
    ds = ds.rename({"lat": "latitude", "lon": "longitude"})
    instant_vars: list[str] = [str(v) for v in ds.data_vars if v not in ["tp", "tp_bc"]]
    accum_vars: list[str] = [str(v) for v in ds.data_vars if v in ["tp", "tp_bc"]]
    pop = get_worldpop("VNM", pop_year)
    raster_bbox = Bbox.from_xarray(ds)
    region_overlap = raster_bbox.overlap_fraction(region.bbox)
    if region_overlap < 0.80:
        raise ValueError(
            f"Insufficient overlap ({region_overlap:.1%}, expected 80%) between input raster and region bbox"
        )
    if raster_bbox < pop.bbox:
        # Crop population to region bbox if region is smaller
        logger.warning(f"""Cropping larger population raster to smaller input raster
    Population bounds: {pop.bbox}
        Raster bounds: {raster_bbox}""")
        pop = pop.crop(raster_bbox)
        post_crop_overlap = raster_bbox.overlap_fraction(pop.bbox)
        logger.info(f"After cropping overlap fraction is {post_crop_overlap:.1%}")
    if not (instant_vars + accum_vars):
        raise ValueError(f"At least one variable must be passed, got {vars!r}")
    if instant_vars:
        logger.info("Performing zonal stats for %r", instant_vars)
        print("Performing zonal stats for", instant_vars)
        assert ds.t2m.notnull().all(), "Null values found in source temperature field"
        with resampled_dataset("remapbil", ds[instant_vars], pop) as remapbil_ds:
            if remapbil_ds.t2m.isnull().any():
                null_frac = remapbil_ds.t2m.isnull().sum() / remapbil_ds.t2m.size
                raise ValueError(
                    f"Null values found in temperature field ({null_frac:.1%}), indicates issues with resampling"
                )
            with multiprocessing.Pool() as pool:
                instant_zs = xr.merge(
                    pool.map(
                        functools.partial(
                            zonal_stats,
                            ds=remapbil_ds,
                            region=region,
                            weights=pop,
                            sims=sims,
                        ),
                        instant_vars,
                    )
                )

    else:
        instant_zs = None
    if accum_vars:
        logger.info("Performing zonal stats for %r", accum_vars)
        print("Performing zonal stats for", accum_vars)
        assert ds.tp.notnull().all(), "Null values found in source precipitation field"
        with resampled_dataset("remapdis", ds[accum_vars], pop) as remapdis_ds:
            if remapdis_ds.tp.isnull().any():
                null_frac = remapdis_ds.tp.isnull().sum() / remapdis_ds.tp.size
                raise ValueError(
                    "Null values found in precipitation field ({null_frac:.1%}), indicates issues with resampling"
                )
            with multiprocessing.Pool() as pool:
                accum_zs = xr.merge(
                    pool.map(
                        functools.partial(
                            zonal_stats,
                            ds=remapdis_ds,
                            region=region,
                            weights=pop,
                            sims=sims,
                        ),
                        accum_vars,
                    )
                )
    else:
        accum_zs = None
    if instant_zs is None:
        return accum_zs
    if accum_zs is None:
        return instant_zs
    return xr.merge([instant_zs, accum_zs])


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
