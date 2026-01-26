"""Utility functions for ecmwf.forecast"""

import logging
import tempfile
from pathlib import Path

from geoglue.region import CountryAdministrativeLevel
import numpy as np
import xarray as xr
import geoglue.zonal_stats
from geoglue import AdministrativeLevel
from geoglue.memoryraster import MemoryRaster
from geoglue.types import Bbox
from geoglue.resample import resample
from tqdm import tqdm

from ...paths import get_path

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

# Variables to select from GRIB
INSTANT_VARS = ["t2m", "d2m", "sp"]
ACCUM_VARS = ["tp"]

# Additional variables that should be treated as accum
# i.e. perform cdo remapdis and area_weighted_sum
ZONAL_STATS_ACCUM_VARS = ["tp", "tp_bc", "spi", "spi_bc", "spei", "spei_bc"]

logger = logging.getLogger(__name__)


def zonal_stats(
    var: str,
    ds: xr.Dataset,
    region: AdministrativeLevel,
    weights: MemoryRaster,
    ensemble_median: bool,
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
    region : geoglue.AdministrativeLevel
        Region for which to calculate zonal statistics
    weights : MemoryRaster
        Uses the specified raster to perform weighted zonal statistics
    ensemble_median : bool
        Whether to perform ensemble median, this speeds up zonal statistics by
        50x (the number of simulations)
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
        if da.name not in ZONAL_STATS_ACCUM_VARS
        else "area_weighted_sum"
    )
    call = (
        f"zonal_stats(da, {region.name}, {operation=}, {weights=}, {ensemble_median=})"
    )
    if ensemble_median:
        za = geoglue.zonal_stats.zonal_stats_xarray(
            da.median("number"), geom, operation, weights, region_col=region.pk
        ).rename(da.name)
        x, y = za.shape
        if x * y == 0:
            raise ValueError(f"Zero dimension DataArray created from {call}", call)
    else:
        za = xr.concat(
            [
                geoglue.zonal_stats.zonal_stats_xarray(
                    da.sel(number=i), geom, operation, weights, region_col=region.pk
                ).rename(da.name)
                for i in range(da.number.size)
            ],
            dim="number",
        )
        za = za.assign_coords(number=range(len(za.number)))
        x, y, z = za.shape
        if x * y * z == 0:  # (time, region, number)
            raise ValueError(f"Zero dimension DataArray created from {call}")
    za.attrs = da.attrs.copy()
    za.attrs["DART_region"] = str(region)
    za.attrs["DART_zonal_stats"] = call
    return za


def forecast_zonal_stats(
    region: CountryAdministrativeLevel, date: str, ensemble_median: bool = True
) -> xr.Dataset:
    corrected_forecast_file = get_path(
        "sources",
        region.name,
        "ecmwf",
        f"{region.name}-{date}-ecmwf.forecast.corrected.nc",
    )
    corrected_forecast_instant = get_path(
        "scratch",
        region.name,
        "ecmwf",
        f"{region.name}-{date}-ecmwf.forecast.corrected.instant.nc",
    )
    corrected_forecast_accum = get_path(
        "scratch",
        region.name,
        "ecmwf",
        f"{region.name}-{date}-ecmwf.forecast.corrected.accum.nc",
    )
    cleanup = [corrected_forecast_instant, corrected_forecast_accum]
    pop_year = int(date.split("-")[0])
    ds = xr.open_dataset(corrected_forecast_file, decode_timedelta=True).rename(
        {"lat": "latitude", "lon": "longitude"}
    )
    instant_vars: list[str] = [
        str(v) for v in ds.data_vars if v not in ZONAL_STATS_ACCUM_VARS
    ]
    accum_vars: list[str] = [
        str(v) for v in ds.data_vars if v in ZONAL_STATS_ACCUM_VARS
    ]

    # write out instant and accum subsets for resampling
    ds[instant_vars].to_netcdf(corrected_forecast_instant)
    ds[accum_vars].to_netcdf(corrected_forecast_accum)
    logger.info(
        "Wrote instant and accum parts: %s, %s",
        corrected_forecast_instant,
        corrected_forecast_accum,
    )
    pop = get_worldpop(region, pop_year)

    # Check raster and population bounding boxes:
    # If one is enclosed in the other, then crop, otherwise
    # compute overlap fraction and show a warning if the bounding
    # boxes have insufficient overlap
    raster_bbox = Bbox.from_xarray(ds)

    if raster_bbox < pop.bbox:
        # Crop population to region bbox if region is smaller
        logger.warning(f"""Cropping larger population raster to smaller input raster
    Population bounds: {pop.bbox}
        Raster bounds: {raster_bbox}""")
        pop = pop.crop(raster_bbox)
        post_crop_overlap = raster_bbox.overlap_fraction(pop.bbox)
        logger.info(f"After cropping overlap fraction is {post_crop_overlap:.1%}")
    elif pop.bbox < raster_bbox:
        logger.warning(f"""Cropping larger climate raster to smaller population raster_bbox
    Population bounds: {pop.bbox}
        Raster bounds: {raster_bbox}""")
        ds = ds.sel(latitude=pop.bbox.lat_slice, longitude=pop.bbox.lon_slice)
    else:
        region_overlap = raster_bbox.overlap_fraction(region.bbox)
        if region_overlap < 0.80 and not (pop.bbox < raster_bbox):
            raise ValueError(
                f"Insufficient overlap ({region_overlap:.1%}, expected 80%) between input raster and region bbox"
            )

    if not (instant_vars + accum_vars):
        raise ValueError(f"At least one variable must be passed, got {vars!r}")
    resampled_instant_path = get_path(
        "scratch",
        region.name,
        "ecmwf",
        f"{region.name}-{date}-ecmwf.forecast.instant_resampled.nc",
    )
    resampled_accum_path = get_path(
        "scratch",
        region.name,
        "ecmwf",
        f"{region.name}-{date}-ecmwf.forecast.accum_resampled.nc",
    )
    if instant_vars:
        logger.info("Performing zonal stats for %r", instant_vars)
        assert ds.t2m.notnull().all(), "Null values found in source temperature field"
        logger.info(
            "Resampling instant variables using CDO [remapbil] to target grid:\n%s",
            pop.griddes,
        )
        resample("remapbil", corrected_forecast_instant, pop, resampled_instant_path)
        cleanup.append(resampled_instant_path)
        remapbil_ds = xr.open_dataset(resampled_instant_path, decode_timedelta=True)
        remapbil_ds = remapbil_ds[instant_vars]
        if remapbil_ds.t2m.isnull().any():
            null_frac = remapbil_ds.t2m.isnull().sum() / remapbil_ds.t2m.size
            raise ValueError(
                f"Null values found in temperature field ({null_frac:.1%}), indicates issues with resampling"
            )
        instant_zs = xr.merge(
            [
                zonal_stats(
                    var,
                    ds=remapbil_ds,
                    region=region,
                    weights=pop,
                    ensemble_median=ensemble_median,
                )
                for var in tqdm(instant_vars, desc="Instant variables")
            ]
        )
    else:
        instant_zs = None
    if accum_vars:
        logger.info("Performing zonal stats for %r", accum_vars)
        assert ds.tp.notnull().all(), "Null values found in source precipitation field"
        logger.info(
            "Resampling accum variables using CDO [remapdis] to target grid:\n%s",
            pop.griddes,
        )
        resample("remapdis", corrected_forecast_accum, pop, resampled_accum_path)
        cleanup.append(resampled_accum_path)
        remapdis_ds = xr.open_dataset(resampled_accum_path, decode_timedelta=True)
        remapdis_ds = remapdis_ds[accum_vars]
        if remapdis_ds.tp.isnull().any():
            null_frac = remapdis_ds.tp.isnull().sum() / remapdis_ds.tp.size
            raise ValueError(
                "Null values found in precipitation field ({null_frac:.1%}), indicates issues with resampling"
            )
        accum_zs = xr.merge(
            [
                zonal_stats(
                    var,
                    ds=remapdis_ds,
                    region=region,
                    weights=pop,
                    ensemble_median=ensemble_median,
                )
                for var in tqdm(accum_vars, desc="Accum variables")
            ]
        )

    else:
        accum_zs = None
    if instant_zs is None:
        return accum_zs
    if accum_zs is None:
        return instant_zs
    for file in cleanup:
        if file.exists():
            file.unlink()
    return xr.merge([instant_zs, accum_zs])


def forecast_grib_to_netcdf(file: Path, bbox: Bbox) -> tuple[xr.Dataset, xr.Dataset]:
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
    bbox : Bbox
        Bounding box to select, this is usually obtained from a geoglue Region object

        .. code-block::

            from geoglue.region import gadm
            region = gadm("VNM", 1)
            print(region.bbox)

    Returns
    -------
    tuple[xr.Dataset, xr.Dataset]
        Tuple of instant and accumulative variables
    """

    sel_kwargs: dict[str, slice] = {
        "latitude": bbox.lat_slice,
        "longitude": bbox.lon_slice,
    }
    ensembles_len = len(
        xr.open_dataset(
            file,
            engine="cfgrib",
            decode_timedelta=True,
            filter_by_keys={"dataType": "pf"},
        ).number
    )
    # number of simulations to correct. We start at 1 because control forecast (cf) counts as sim 0
    sims = np.arange(1, ensembles_len, 5)
    # and perturbed sim goes to 1 to n
    if ensembles_len not in sims:
        sims = np.append(sims, ensembles_len)

    # Loop for extracting data in grib and put it into netcdf. It is important
    # to note that xarray does not let extract variables from grib that are
    # considered in different levels (such as surface pressure and two meter
    # temperature) or simulations (control and perturbed simulations) at the
    # same time. Hence we need to extract temperatures and the rest of the
    # variables separately. And the same with control and perturbed simulations

    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info("Extracting control and perturbed forecast (0)")
        interc = xr.merge(
            [
                xr.open_dataset(
                    file,
                    engine="cfgrib",
                    decode_timedelta=True,
                    filter_by_keys={"dataType": "cf"},
                ).sel(sel_kwargs),
                xr.open_dataset(
                    file,
                    engine="cfgrib",
                    decode_timedelta=True,
                    filter_by_keys={"dataType": "cf", "shortName": ["2t", "2d"]},
                ).sel(sel_kwargs),
            ],
            compat="override",
        )

        # Load perturbed forecast (pf)
        interp = xr.merge(
            [
                xr.open_dataset(
                    file,
                    engine="cfgrib",
                    decode_timedelta=True,
                    filter_by_keys={"dataType": "pf"},
                ).sel(**sel_kwargs, number=slice(sims[0], sims[1])),
                xr.open_dataset(
                    file,
                    engine="cfgrib",
                    decode_timedelta=True,
                    filter_by_keys={"dataType": "pf", "shortName": ["2t", "2d"]},
                ).sel(**sel_kwargs, number=slice(sims[0], sims[1])),
            ],
            compat="override",
        )
        interp = xr.concat([interc, interp], dim="number")
        interp.to_netcdf(Path(temp_dir) / "intersim_0.nc")
        for i in range(1, len(sims) - 1):
            logger.info("Extracting simulations from %d -- %d", sims[i], sims[i + 1])
            interp = xr.merge(
                [
                    xr.open_dataset(
                        file,
                        engine="cfgrib",
                        decode_timedelta=True,
                        filter_by_keys={"dataType": "pf"},
                    ).sel(**sel_kwargs, number=slice(sims[i] + 1, sims[i + 1])),
                    xr.open_dataset(
                        file,
                        engine="cfgrib",
                        decode_timedelta=True,
                        filter_by_keys={"dataType": "pf", "shortName": ["2t", "2d"]},
                    ).sel(**sel_kwargs, number=slice(sims[i] + 1, sims[i + 1])),
                ],
                compat="override",
            )
            interp.to_netcdf(Path(temp_dir) / f"intersim_{i}.nc")

        ds = xr.open_dataset(Path(temp_dir) / "intersim_0.nc", decode_timedelta=True)
        for i in range(1, len(sims) - 1):
            ds = xr.concat(
                [
                    ds,
                    xr.open_dataset(
                        Path(temp_dir) / f"intersim_{i}.nc", decode_timedelta=True
                    ),
                ],
                dim="number",
            )  # concatenate intermediate files

    return ds[INSTANT_VARS], ds[ACCUM_VARS]
