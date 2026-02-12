import logging

import geoglue
import geoglue.zonalstats
import geopandas as gpd
import xarray as xr
from geoglue.memoryraster import MemoryRaster
from geoglue.resample import resample
from geoglue.util import read_raster
from rasterio.enums import Resampling
from tqdm import trange

from dart_pipeline.metrics.worldpop import get_worldpop
from dart_pipeline.paths import get_path
from dart_pipeline.util import msg

from . import register_metrics, register_process

logger = logging.getLogger(__name__)

register_metrics(
    "wrf_downscale",
    description="Downscaled metrics for HCMC using WRF",
    metrics={
        "precip": {
            "url": "",
            "long_name": "Daily downscaled precipitation for HCMC",
            "units": "mm",
            # We produce outputs at minimum admin1 resolution, unlikely
            # that any administrative area will have population greater than this
            "valid_min": 0,
        },
    },
)


def parse_year_range(date: str) -> tuple[int, int]:
    try:
        ystart, yend = date.split("-")
        ystart = int(ystart)
        yend = int(yend)
    except ValueError:
        raise ValueError("Specify date as a year range, e.g. 2000-2020")
    if ystart >= yend:
        raise ValueError(f"Year end {yend} must be greater than year start {ystart}")
    return ystart, yend


def process_precip(
    region: geoglue.AdministrativeLevel,
    year: int,
) -> xr.DataArray:
    geom = gpd.read_file("data/sources/VNM/HCMC/gadm41_HCMC_1.shp")
    wrf_precip_ds = (
        read_raster("data/sources/VNM/wrf/HCM_precip_2000_2024.nc")
        .sel(time=str(year))
        .clip(min=0)
    )

    da = wrf_precip_ds["precip"]

    # Build a target raster on the SAME grid as your precip
    pop = get_worldpop(region, year)

    weights = pop.resample(MemoryRaster.from_xarray(da.isel(time=0)), Resampling.sum)

    zonal_agged_da = geoglue.zonalstats.zonalstats(
        da, geom, "area_weighted_sum", weights
    )

    return zonal_agged_da


@register_process("wrf_downscale.precip")
def wrf_downscale_precip_process(
    region: geoglue.AdministrativeLevel,
    date: str,
) -> xr.Dataset:
    ystart, yend = parse_year_range(date)
    yrange_str = f"{ystart}-{yend}"

    msg("==> Retrieving Worldpop population:", yrange_str)
    for year in range(ystart, yend + 1):
        get_worldpop(region, year)
    msg("==> Worldpop data retrieved")

    ds_lst: list[xr.DataArray] = []

    msg("==> Calculating downscale precipitation (daily):", yrange_str)
    for year in trange(ystart, yend + 1, desc="wrf_downscale.precip"):
        y_output = get_path(
            "output",
            region.name,
            "wrf_downscale",
            f"{region.name}-{region.admin}-{year}-wrf_downscale.precip.nc",
        )
        processed_precip = process_precip(region, year).rename("wrf_downscale.precip")
        processed_precip.to_netcdf(y_output)
        ds_lst.append(processed_precip)

    ds = xr.merge(ds_lst)

    return ds
