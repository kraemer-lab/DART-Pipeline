import logging
from typing import Literal

import xarray as xr
from geoglue import AdministrativeLevel
from geoglue.resample import resampled_dataset
from geoglue.util import read_raster
from geoglue.zonalstats import zonalstats
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


def read_wrf_data() -> xr.Dataset:
    ds_list: list[xr.Dataset | xr.DataArray] = []

    ds_2000_2024 = read_raster("data/VNM/wrf/HCM_precip_2000_2024.nc")
    ds_2025 = read_raster("data/VNM/wrf/HCM_precip_2025.nc")

    ds_list.append(ds_2000_2024)
    ds_list.append(ds_2025)

    return xr.merge(ds_list)


def process_precip(
    region: AdministrativeLevel,
    year: int,
    temporal_resolution: Literal["weekly", "daily"] = "daily",
) -> xr.DataArray:
    # setup gpd for HCMC
    geom = region.read()
    geom = geom[geom["GID_1"] == "VNM.25_1"].reset_index(drop=True)

    # get WRF HCMC data
    wrf_precip_ds = read_wrf_data().sel(time=str(year)).clip(min=0)

    # get WorldPop data as weights
    weights = get_worldpop(region, year)

    with resampled_dataset("remapdis", wrf_precip_ds, weights) as resampled_precip:
        logger.info("Starting zonal statistics for WRF downscaled")
        zonal_agged_da = zonalstats(
            resampled_precip["precip"], geom, "area_weighted_sum", weights
        ).astype("float32")

        for dt_name in ("date", "valid_time"):
            if (dt_name in zonal_agged_da.dims) or (dt_name in zonal_agged_da.coords):
                zonal_agged_da = zonal_agged_da.rename({dt_name: "time"})
                break

    return zonal_agged_da


@register_process("wrf_downscale.precip")
def wrf_downscale_precip_process(
    region: AdministrativeLevel,
    date: str,
    temporal_resolution: Literal["weekly", "daily"] = "daily",
) -> xr.Dataset:
    ystart, yend = parse_year_range(date)
    yrange_str = f"{ystart}-{yend}"

    msg("==> Retrieving Worldpop population:", yrange_str)
    for year in range(ystart, yend + 1):
        get_worldpop(region, year)
    msg("==> Worldpop data retrieved")

    ds_lst: list[xr.DataArray] = []

    msg(f"==> Calculating downscale precipitation ({temporal_resolution}):", yrange_str)
    for year in trange(ystart, yend + 1, desc="wrf_downscale.precip"):
        y_output = get_path(
            "output",
            region.name,
            "wrf_downscale",
            f"{region.name}-{region.admin}-{year}-wrf_downscale.precip.nc",
        )
        processed_precip = process_precip(region, year, temporal_resolution).rename(
            "wrf_downscale.precip"
        )
        processed_precip.to_netcdf(y_output)
        ds_lst.append(processed_precip)

    ds = xr.merge(ds_lst)

    return ds
