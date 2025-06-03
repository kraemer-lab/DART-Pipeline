"""ERA5 collate virtual metric

Takes existing processed metrics as netCDF and concatenates them into one file
"""

import logging
import itertools
from pathlib import Path

import pandas as pd
import xarray as xr
from geoglue.util import get_first_monday, get_last_sunday, find_unique_time_coord

from ...paths import get_path

logger = logging.getLogger(__name__)

GLOB_DAILY = "*-20*-era5.*.daily*.nc"
GLOB_WEEKLY = "*-20*-era5.*.weekly*.nc"


class MetricCollection:
    """Represents a collection of output metrics"""

    def __init__(self, region: str, data_path: Path | None = None):
        self.region_without_admin = region.split("-")[0]
        self.region = region
        root_path = data_path or get_path("output", self.region_without_admin, "era5")
        data = []
        for d in itertools.chain(
            root_path.glob(GLOB_DAILY), root_path.glob(GLOB_WEEKLY)
        ):
            if d.suffix != ".nc":
                continue
            parts = d.stem.split("-")
            metric = parts.pop()
            year = int(parts.pop())
            _region = "-".join(parts)
            data.append((_region, year, metric, "daily" in metric, d))
        df = pd.DataFrame(
            data,
            columns=["region", "year", "metric", "is_daily", "path"],  # type: ignore
        )
        self.data = df[df.region == region]  # type: ignore
        if self.data.empty:
            raise ValueError(
                f"No match found for {region=}, might be missing admin level like VNM-2"
            )
        self.min_year = self.data.year.min()
        self.max_year = self.data.year.max()

    def collate_metric(
        self, metric: str, yrange: tuple[int, int] | None = None
    ) -> xr.DataArray:
        if yrange is None:
            ymin, ymax = self.min_year, self.max_year
        else:
            ymin, ymax = yrange
        assert ymax >= ymin, "Year end must be greater that year beginning"
        logger.info("Collating %s for years %d-%d", metric, ymin, ymax)
        # check completeness, every year from ymin to ymax must be present
        df = self.data[self.data.metric == metric]
        if df.empty:  # type: ignore
            raise ValueError(
                f"No data found for {metric=} from {ymin}-{ymax} in {self.region}"
            )
        if len(set(df.is_daily)) > 1:  # type: ignore
            raise ValueError(
                f"Combining the same {metric=} at both weekly and daily levels not supported"
            )
        if not (set(range(ymin, ymax + 1)) <= set(df.year)):  # type: ignore
            raise ValueError(
                f"Contiguous years not present for {metric=} from {ymin}-{ymax} in {self.region}"
            )
        df = df[(df.year >= ymin) & (df.year <= ymax)].sort_values("year")  # type: ignore

        # concat and merge
        da = xr.open_dataarray(df.iloc[0].path)
        time_dim = find_unique_time_coord(da)
        for i in range(1, len(df)):
            da_y = xr.open_dataarray(df.iloc[i].path)
            da = xr.concat([da, da_y], dim=time_dim)
        if not df.iloc[0].is_daily:
            return da  # already aggregated to weekly timestep
        da = da.sel(
            {time_dim: slice(str(get_first_monday(ymin)), str(get_last_sunday(ymax)))}
        )
        if "sum" in metric:
            logger.info("Resampling %s to weekly timestep (sum)", metric)
            da_w = da.resample({time_dim: "W-MON"}, closed="left", label="left").sum()
            da_w.attrs["cell_methods"] = (
                da_w.attrs["cell_methods"] + f" {time_dim}: sum (interval: 1 week)"
            )
        else:
            logger.info("Resampling %s to weekly timestep (mean)", metric)
            da_w = da.resample({time_dim: "W-MON"}, closed="left", label="left").mean()
            da_w.attrs["cell_methods"] = (
                da_w.attrs["cell_methods"] + f" {time_dim}: mean (interval: 1 week)"
            )
        return da_w

    def collate(self, yrange: tuple[int, int] | None = None) -> xr.Dataset:
        metrics = set(self.data.metric)
        return xr.merge(self.collate_metric(m, yrange) for m in metrics)
