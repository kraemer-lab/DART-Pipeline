"""Worldpop population count data, aggregated to administrative levels"""

import string
import warnings
from typing import Literal
from urllib.parse import urljoin

import requests
import xarray as xr
import pandas as pd
from geoglue.region import gadm
from geoglue.memoryraster import MemoryRaster
from functools import cache

from ..paths import get_path
from ..util import iso3_admin_unpack, download_file
from ..metrics import register_metrics, register_fetch, register_process

WORLDPOP_ROOT = "https://data.worldpop.org/GIS/Population/"

WORLDPOP_DATASETS = {
    "default": "Global_2000_2020_1km_UNadj/$year/$iso3/${iso3_lower}_ppp_${year}_1km_Aggregated_UNadj.tif",
    "future": "Global_2015_2030/R2024B/$year/$iso3/v1/1km_ua/unconstrained/${iso3_lower}_pop_${year}_UC_1km_R2024B_UA_v1.tif",
}
WORLDPOP_YEAR_RANGE: dict[str, tuple[int, int]] = {
    "default": (2000, 2020),
    "future": (2015, 2030),
}

register_metrics(
    "worldpop",
    description="WorldPop population data",
    metrics={
        "pop_count": {
            "url": "https://hub.worldpop.org/geodata/listing?id=75",
            "long_name": "WorldPop population count",
            "units": "unitless",
            "license": "CC-BY-4.0",
            # We produce outputs at minimum admin1 resolution, unlikely
            # that any administrative area will have population greater than this
            "valid_min": 0,
            "citation": """
             WorldPop (www.worldpop.org - School of Geography and Environmental
             Science, University of Southampton; Department of Geography and
             Geosciences, University of Louisville; Departement de Geographie,
             Universite de Namur) and Center for International Earth Science
             Information Network (CIESIN), Columbia University (2018). Global
             High Resolution Population Denominators Project - Funded by The
             Bill and Melinda Gates Foundation (OPP1134076).
             https://dx.doi.org/10.5258/SOTON/WP00671
             """,
        },
    },
)


@cache
def get_worldpop(iso3: str, year: int, dataset: str | None = None) -> MemoryRaster:
    """
    Downloads and returns WorldPop population raster (1km resolution)
    for a particular year and WorldPop dataset.

    Parameters
    ----------
    iso3 : str
        ISO3 code of country
    year : int
        Year for which to download data
    dataset : str | None

        Dataset or template string to expand, default=None. Currently there are two
        pre-defined datasets, 'default' and 'future'. Alternatively, one
        may supply a template URL fragment that will be expanded and fetched.

        If dataset=None, autoselect the best dataset depending upon year: if within
        2000-2020 select the 'default' dataset, or else if within 2030, select
        the future dataset.

        default
            Default WorldPop dataset, UN adjusted unconstrained estimates,
            1km resolution, 2000-2020.

            Template: ``Global_2000_2020_1km_UNadj/$year/$iso3/${iso3_lower}_ppp_${year}_1km_Aggregated_UNadj.tif``

        future
            WorldPop projection datasets, unconstrained, 1km resolution, 2015-2030

            Template: ``Global_2015_2030/R2024B/$year/$iso3/v1/1km_ua/unconstrained/${iso3_lower}_pop_${year}_UC_1km_R2024B_UA_v1.tif``

        template string
            An arbitrary template string with the variables ``$iso3``, ``$year`` and ``$iso3_lower``
            can be passed as a parameter here, where ``$iso3_lower`` is the lower case version
            of the ISO3 code. Template variables must be enclosed in braces where required to
            avoid ambiguity, e.g. ``${iso3_lower}_v1``. The template will be expanded and
            appended to the base WorldPop URL: https://data.worldpop.org/GIS/Population/

    Returns
    -------
    MemoryRaster
        MemoryRaster representing the population data
    """
    if dataset is None:
        if 2000 <= year <= 2020:
            dataset = "default"
        if 2021 <= year <= 2030:
            dataset = "future"
    if dataset is None:
        raise ValueError(
            f"No pre-defined dataset found for {year}, consider providing a URL template"
        )

    if dataset in ["default", "future"]:
        ystart, yend = WORLDPOP_YEAR_RANGE[dataset]
        if year < ystart or year > yend:
            raise ValueError(
                f"Worldpop population data for {dataset=} is only available from {ystart}-{yend}"
            )
    if dataset == "future" and 2000 <= year <= 2020:
        warnings.warn(
            f"get_worldpop(): dataset 'future' selected for {year=}, consider using actual data using dataset='default'"
        )

    path_population = get_path("sources", iso3, "worldpop")
    template = (
        string.Template(WORLDPOP_DATASETS[dataset])
        if dataset in WORLDPOP_DATASETS
        else string.Template(dataset)
    )
    assert set(template.get_identifiers()) <= {"iso3", "iso3_lower", "year"}
    url_fragment = template.substitute(
        {"iso3": iso3.upper(), "iso3_lower": iso3.lower(), "year": year}
    )
    path_population.mkdir(parents=True, exist_ok=True)
    url = urljoin(WORLDPOP_ROOT, url_fragment)
    output_path = path_population / url.split("/")[-1]
    if output_path.exists() or download_file(url, output_path):
        return MemoryRaster.read(output_path)
    else:
        raise requests.ConnectionError(f"Failed to download {url=}")


@register_fetch("worldpop.pop_count")
def worldpop_pop_count_fetch(iso3: str, date: str) -> Literal[False]:
    "Fetch worldpop count data"

    if "-" in iso3:
        iso3, _ = iso3.split("-")
    year = int(date)
    _ = get_worldpop(iso3, year)

    return False  # ensures that dart-pipeline get skips processing


@register_process("worldpop.pop_count")
def worldpop_pop_count_process(iso3: str, date: str) -> xr.DataArray:
    iso3, admin = iso3_admin_unpack(iso3)
    year = int(date)
    population = get_worldpop(iso3, year)
    region = gadm(iso3, admin)
    geom = region.read()
    include_cols = [c for c in geom.columns if c != "geometry"]
    df = population.zonal_stats(geom, "sum", include_cols=include_cols).rename(
        columns={"sum": "value", "GID_0": "ISO3", region.pk: "region"}
    )
    da = xr.DataArray(pd.Series(df.value, index=df.region, name="pop"))
    da.attrs.update(
        {
            "long_name": "Worldpop population count",
            "units": "1",
            "DART_region": str(region),
        }
    )
    return da.expand_dims(time=[pd.Timestamp(str(year))])
