from pathlib import Path
from typing import NamedTuple, TypedDict, Literal

import xarray as xr


class ERA5HourlyPath(NamedTuple):
    instant: Path | None
    accum: Path | None

    def get_instant(self) -> xr.Dataset:
        return xr.open_dataset(self.instant)

    def get_accum(self) -> xr.Dataset:
        return xr.open_dataset(self.accum)


class ERA5Dataset(NamedTuple):
    instant: xr.Dataset
    accum: xr.Dataset


class CDSRequest(TypedDict):
    product_type: list[str]
    variable: list[str]
    year: list[str]
    month: list[str]
    day: list[str]
    time: list[str]
    area: list[int]
    data_format: Literal["netcdf"]
    download_format: Literal["unarchived"]
