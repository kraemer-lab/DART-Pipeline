import logging
from pathlib import Path
from typing import Literal

import geopandas as gpd
import xarray as xr
from geoglue import AdministrativeLevel
from geoglue.resample import resampled_dataset
from geoglue.util import read_raster
from geoglue.zonalstats import zonalstats
from tqdm import trange

from ..paths import get_path
from ..util import msg
from . import register_metrics, register_process
from .worldpop import get_worldpop

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


def read_wrf_data(region: AdministrativeLevel) -> xr.Dataset:
    ds_list: list[xr.Dataset | xr.DataArray] = []

    parent_dir = get_path("sources", region.name, "wrf_downscale")

    ds_2000_2024 = read_raster(parent_dir / "HCM_precip_2000_2024.nc")
    ds_2025 = read_raster(parent_dir / "HCM_precip_2025.nc")

    ds_list.append(ds_2000_2024)
    ds_list.append(ds_2025)

    return xr.merge(ds_list)


def process_precip(
    region: AdministrativeLevel,
    year: int,
    wrf_precip_ds: xr.Dataset,
    temporal_resolution: Literal["weekly", "daily"] = "daily",
) -> xr.DataArray:
    # setup gpd for HCMC
    geom = region.read()
    hcmc_geom = geom[geom.GID_1 == "VNM.25_1"].reset_index(drop=True)
    gid_lookup = hcmc_geom[f"GID_{region.admin}"].astype(str).to_numpy()

    # use WorldPop data as weights
    weights = get_worldpop(region, year).fillna(0)

    # crop WorldPop data by HCMC geom extent
    geom_bounds = hcmc_geom.union_all().bounds
    cropped_weights = weights.where(
        (weights.latitude > geom_bounds[1])
        & (weights.latitude < geom_bounds[3])
        & (weights.longitude > geom_bounds[0])
        & (weights.longitude < geom_bounds[2]),
        drop=True,
    )

    with resampled_dataset(
        "remapdis", wrf_precip_ds, cropped_weights
    ) as resampled_precip:
        logger.info("Starting zonal statistics for WRF downscaled")
        zonal_agged_da = zonalstats(
            resampled_precip["precip"], hcmc_geom, "area_weighted_sum", cropped_weights
        ).astype("float32")

        for dt_name in ("date", "valid_time"):
            if (dt_name in zonal_agged_da.dims) or (dt_name in zonal_agged_da.coords):
                zonal_agged_da = zonal_agged_da.rename({dt_name: "time"})
                break

    zonal_agged_da = zonal_agged_da.assign_coords(region=("region", gid_lookup))

    return zonal_agged_da


@register_process("wrf_downscale.precip", multiple_years=True)
def wrf_downscale_precip_process(
    region: AdministrativeLevel,
    date: str,
    temporal_resolution: Literal["weekly", "daily"] = "daily",
    overwrite: bool = True,
) -> list[Path]:
    ystart, yend = parse_year_range(date)
    yrange_str = f"{ystart}-{yend}"

    msg("==> Retrieving Worldpop population:", yrange_str)
    for year in range(ystart, yend + 1):
        get_worldpop(region, year)
    msg("==> Worldpop data retrieved")

    msg("==> Retrieving WRF downscaled precipitation data")
    wrf_precip_ds_full = read_wrf_data(region).clip(min=0)
    msg("==> WRF downscaled precipitation data retrieved")

    ds_lst: list[xr.DataArray] = []

    msg(f"==> Processing downscaled precipitation ({temporal_resolution}):", yrange_str)
    for year in trange(ystart, yend + 1, desc="wrf_downscale.precip"):
        y_output = get_path(
            "output",
            region.name,
            "wrf_downscale",
            f"{region.name}-{region.admin}-{year}-wrf_downscale.precip.nc",
        )
        wrf_precip_ds_year = wrf_precip_ds_full.sel(time=str(year))
        processed_precip = process_precip(
            region, year, wrf_precip_ds_year, temporal_resolution
        ).rename("wrf_downscale.precip")
        processed_precip.to_netcdf(y_output)
        ds_lst.append(processed_precip)

    output = get_path(
        "output",
        region.name,
        "wrf_downscale",
        f"{region.name}-{region.admin}-{ystart}-{yend}-wrf_downscale.precip.nc",
    )

    ds = xr.merge(ds_lst)
    ds.attrs["DART_region"] = (
        f"{region.name} {region.pk} {region.tz} {region.bbox.int()}"
    )
    ds.to_netcdf(output)

    return [output]
