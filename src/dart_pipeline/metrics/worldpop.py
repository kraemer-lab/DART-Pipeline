"""Worldpop population count data, aggregated to administrative levels"""

import logging
import re
import string
import warnings
from pathlib import Path
from typing import Literal
from urllib.parse import urljoin

import pandas as pd
import psutil
import requests
import xarray as xr

# from functools import cache
from cachetools import LRUCache, cached
from geoglue.region import (
    AdministrativeLevel,
    BaseCountry,
    BaseRegion,
    CountryAdministrativeLevel,
)
from geoglue.util import read_geotiff
from geoglue.zonalstats import zonalstats

from ..metrics import (
    get_name_cfattrs,
    register_fetch,
    register_metrics,
    register_process,
)
from ..paths import get_path
from ..util import download_file

logger = logging.getLogger(__name__)

WORLDPOP_ROOT = "https://data.worldpop.org/GIS/Population/"

WORLDPOP_DATASETS = {
    "default": "Global_2000_2020_1km_UNadj/$year/$iso3/${iso3_lower}_ppp_${year}_1km_Aggregated_UNadj.tif",
    "future": "Global_2015_2030/R2024B/$year/$iso3/v1/1km_ua/unconstrained/${iso3_lower}_pop_${year}_UC_1km_R2024B_UA_v1.tif",
}
WORLDPOP_YEAR_RANGE: dict[str, tuple[int, int]] = {
    "default": (2000, 2020),
    "future": (2015, 2030),
}

WORLDPOP_AGE_SEX_ROOT = "https://data.worldpop.org/GIS/AgeSex_structures/"
WORLDPOP_AGE_SEX_TEMPLATE = (
    "Global_2015_2030/R2025A/$year/$iso3/v1/1km_ua/constrained/"
    "${iso3_lower}_${sex}_${age}_${year}_CN_1km_R2025A_UA_v1.tif"
)
WORLDPOP_AGE_SEX_YEAR_RANGE = (2015, 2030)
WORLDPOP_AGE_GROUPS = [0, 1, *range(5, 95, 5)]
WORLDPOP_SEXES = {"f": "female", "m": "male", "t": "both"}
WORLDPOP_DEFAULT_SEXES = ["f", "m"]

AGE_SELECTION_USAGE = (
    "use a comma separated list of ages such as 5, inclusive age ranges such "
    "as 0-5, or age ranges with no upper limit such as 65+, e.g. age=0-5,65+"
)

MAX_MEMORY = psutil.virtual_memory().total
CACHE_MAXSIZE = 0.8 * MAX_MEMORY

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
        "age_sex_structure": {
            "url": "https://hub.worldpop.org/geodata/listing?id=139",
            "long_name": "WorldPop population count by sex and age group",
            "units": "1",
            "license": "CC-BY-4.0",
            "valid_min": 0,
            "citation": """
             Bondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W.,
             McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk
             B., Brinkhoff T., Tatem A., Sorichetta A. (2025). Estimates of
             2015-2030 total number of people per grid square broken down by
             gender and age groupings at a resolution of 30 arc (approximately
             1km at the equator) R2025A version v1. Global Demographic Data
             Project - Funded by The Bill and Melinda Gates Foundation
             (INV-045237). WorldPop - School of Geography and Environmental
             Science, University of Southampton.
             https://dx.doi.org/10.5258/SOTON/WP00842
             """,
        },
    },
)


def sizeof_dataarray(da: xr.DataArray) -> int:
    return da.nbytes


get_worlpop_cache = LRUCache(
    maxsize=CACHE_MAXSIZE,
    # asizeof here handle nested size instead of shallow (which is the case for sys.getsizeof)
    getsizeof=sizeof_dataarray,
)


# TODO: check the hash of 2 get_worldpop calls with the same arguments
@cached(get_worlpop_cache)
def get_worldpop(
    region: BaseCountry,
    year: int,
    dataset: str | None = None,
    nodata_impute: None | int = None,
) -> xr.DataArray:
    """
    Downloads and returns WorldPop population raster (1km resolution)
    for a particular year and WorldPop dataset.

    Parameters
    ----------
    region : geoglue.region.BaseCountry
        Country for which to download population data. Sub-regions of a country
        are not currently supported
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
    xr.DataArray
        xr.DataArray representing the population data
    """
    # - Use iso3 field for data fetching from worldpop
    # - Name should be the name of the custom shapefile
    # iso3 = region.name.upper()
    # iso3_lower = iso3.lower()
    assert region.iso3 is not None
    iso3 = region.iso3.upper()
    iso3_lower = iso3.lower()

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

    path_population = get_path("sources", region.iso3, "worldpop")
    template = (
        string.Template(WORLDPOP_DATASETS[dataset])
        if dataset in WORLDPOP_DATASETS
        else string.Template(dataset)
    )
    assert set(template.get_identifiers()) <= {"iso3", "iso3_lower", "year"}
    url_fragment = template.substitute(
        {"iso3": iso3, "iso3_lower": iso3_lower, "year": year}
    )
    path_population.mkdir(parents=True, exist_ok=True)
    url = urljoin(WORLDPOP_ROOT, url_fragment)
    output_path = path_population / url.split("/")[-1]
    if output_path.exists() or download_file(url, output_path):
        gtiff = read_geotiff(output_path)
        if nodata_impute is not None:
            return gtiff.fillna(nodata_impute)
        else:
            return gtiff
    else:
        raise requests.ConnectionError(f"Failed to download {url=}")


@register_fetch("worldpop.pop_count")
def worldpop_pop_count_fetch(region: BaseCountry, date: str) -> Literal[False]:
    "Fetch worldpop count data"

    year = int(date)
    _ = get_worldpop(region, year)

    return False  # ensures that dart-pipeline get skips processing


@register_process("worldpop.pop_count")
def worldpop_pop_count_process(
    region: CountryAdministrativeLevel, date: str
) -> xr.DataArray:
    year = int(date)
    population = get_worldpop(region, year)
    geom = region.read()
    df = (
        zonalstats(population, geom, "sum")
        .to_dataframe()
        .reset_index()
        .rename(columns={"band_data": "value"})
    )
    # print(df[["region", "value"]])

    series = (
        df[["region", "value"]]
        .set_index("region")
        .value.rename("pop")
        .astype("float32")
    )
    da = xr.DataArray(series)
    da.attrs.update(
        {
            "long_name": "Worldpop population count",
            "units": "1",
            "DART_region": str(region),
        }
    )
    return da.expand_dims(time=[pd.Timestamp(str(year))])


def age_group_end(age: int) -> int | None:
    """Returns the last age in years of a WorldPop age group from its lower bound

    The last age is inclusive, e.g. 4 for the 1-4 group. Returns None for the
    open ended 90+ group.
    """
    if age not in WORLDPOP_AGE_GROUPS:
        raise ValueError(
            f"Invalid WorldPop age group {age=}, valid: {WORLDPOP_AGE_GROUPS}"
        )
    i = WORLDPOP_AGE_GROUPS.index(age)
    return WORLDPOP_AGE_GROUPS[i + 1] - 1 if i + 1 < len(WORLDPOP_AGE_GROUPS) else None


def age_group_label(age: int) -> str:
    "Returns label for a WorldPop age group from its lower bound in years"
    end = age_group_end(age)
    if end is None:
        return f"{age}+"
    if end == 0:
        return "<1"
    return f"{age}-{end}"


def parse_sexes(sex: str | None) -> list[str]:
    """Parses the ``sex`` parameter of worldpop.age_sex_structure

    Parameters
    ----------
    sex : str | None
        Comma separated codes: ``f`` (female), ``m`` (male) or ``t`` (both).
        If None, uses ``f,m``.

    Returns
    -------
    list[str]
        Codes without duplicates, in the order f, m, t
    """
    if sex is None:
        return list(WORLDPOP_DEFAULT_SEXES)
    valid = ", ".join(f"{k} ({v})" for k, v in WORLDPOP_SEXES.items())
    codes = {s.strip().lower() for s in str(sex).split(",") if s.strip()}
    if not codes or codes - set(WORLDPOP_SEXES):
        raise ValueError(
            f"Invalid {sex=} for worldpop.age_sex_structure, use a comma "
            f"separated list of {valid}, such as sex=f,m or sex=t"
        )
    return [s for s in WORLDPOP_SEXES if s in codes]



def _parse_age_range(item: str) -> tuple[int, int | None]:
    """Parses one item of the age parameter into an inclusive range of ages

    ``5`` is (5, 5), ``0-5`` is (0, 5) and ``65+`` is (65, None), where None
    means there is no upper limit.
    """
    match = re.fullmatch(r"(\d+)(?:\s*-\s*(\d+)|\s*(\+))?", item)
    if match is None:
        raise ValueError(f"could not read {item!r}, {AGE_SELECTION_USAGE}")
    start = int(match.group(1))
    if match.group(3) is not None:
        return start, None
    end = int(match.group(2)) if match.group(2) is not None else start
    if end < start:
        raise ValueError(f"range {item!r} ends before it starts")
    return start, end


def age_groups_covering(start: int, end: int | None) -> list[int]:
    """Returns the fewest WorldPop age groups that cover an inclusive age range

    Parameters
    ----------
    start : int
        First age in years
    end : int | None
        Last age in years, inclusive. None means there is no upper limit.

    Returns
    -------
    list[int]
        First ages of the age groups that overlap the range, e.g.
        ``[0, 1, 5]`` (<1, 1-4, 5-9) for ages 0 to 5
    """
    return [
        a
        for a in WORLDPOP_AGE_GROUPS
        if (end is None or a <= end)
        and ((group_end := age_group_end(a)) is None or group_end >= start)
    ]


def _describe_ages(ages: list[int]) -> str:
    "Describes the ages covered by consecutive age groups, e.g. 0-9 or 65+"
    end = age_group_end(ages[-1])
    return f"{ages[0]}+" if end is None else f"{ages[0]}-{end}"


def parse_age_groups(age: str | int | None) -> list[int]:
    """Parses the ``age`` parameter of worldpop.age_sex_structure

    Selects the fewest WorldPop age groups (<1, 1-4, 5-9, ..., 85-89, 90+) that
    cover the requested ages. WorldPop does not split age groups further, so
    the selected groups can cover more ages than requested: ``0-5`` selects
    <1, 1-4 and 5-9, which covers ages 0-9. A warning is logged when this happens.

    Parameters
    ----------
    age : str | int | None
        Comma separated items. Each item is one of:

        - an age, such as ``3``, which selects the group with that age (1-4)
        - an inclusive age range, such as ``0-5``
        - an age range with no upper limit, such as ``65+``

        If None, selects all age groups.

    Returns
    -------
    list[int]
        First ages of the selected age groups, without duplicates, in ascending
        order. These are the values of the ``age`` coordinate in the output.
    """
    if age is None:
        return list(WORLDPOP_AGE_GROUPS)
    items = [a.strip() for a in str(age).split(",") if a.strip()]
    if not items:
        raise ValueError(
            f"Invalid {age=} for worldpop.age_sex_structure, {AGE_SELECTION_USAGE}"
        )
    ages: set[int] = set()
    for item in items:
        try:
            start, end = _parse_age_range(item)
        except ValueError as e:
            raise ValueError(
                f"Invalid {age=} for worldpop.age_sex_structure: {e}"
            ) from None
        groups = age_groups_covering(start, end)
        requested = f"{start}+" if end is None else f"{start}-{end}"
        # a single age selects the age group with that age, only warn for ranges
        if not item.isdigit() and (covered := _describe_ages(groups)) != requested:
            logger.warning(
                "age=%s: WorldPop age groups %s cover ages %s, more than requested",
                item,
                ", ".join(age_group_label(a) for a in groups),
                covered,
            )
        ages.update(groups)
    return sorted(ages)


def worldpop_age_sex_year(date: str) -> int:
    "Parses and validates the year for WorldPop age and sex structures"
    ystart, yend = WORLDPOP_AGE_SEX_YEAR_RANGE
    try:
        year = int(date)
    except ValueError:
        raise ValueError(
            f"WorldPop age and sex structures need a single year, such as 2020, got {date=}"
        ) from None
    if not ystart <= year <= yend:
        raise ValueError(
            f"WorldPop age and sex structures (R2025A) are only available for {ystart}-{yend}, got {year=}"
        )
    return year


def worldpop_age_sex_url(iso3: str, year: int, sex: str, age: int) -> str:
    """Returns the download URL of one WorldPop age and sex structure raster

    Parameters
    ----------
    iso3 : str
        ISO3 code of the country
    year : int
        Year, from 2015 to 2030
    sex : str
        One of ``f`` (female), ``m`` (male) or ``t`` (both)
    age : int
        Lower bound of the age group in years, one of ``WORLDPOP_AGE_GROUPS``
    """
    if sex not in WORLDPOP_SEXES:
        raise ValueError(f"Invalid {sex=}, valid: {list(WORLDPOP_SEXES)}")
    if age not in WORLDPOP_AGE_GROUPS:
        raise ValueError(
            f"Invalid WorldPop age group {age=}, valid: {WORLDPOP_AGE_GROUPS}"
        )
    url_fragment = string.Template(WORLDPOP_AGE_SEX_TEMPLATE).substitute(
        {
            "iso3": iso3.upper(),
            "iso3_lower": iso3.lower(),
            "year": year,
            "sex": sex,
            "age": f"{age:02d}",
        }
    )
    return urljoin(WORLDPOP_AGE_SEX_ROOT, url_fragment)


def get_worldpop_age_sex_path(
    region: BaseRegion, year: int, sex: str, age: int
) -> Path:
    """Returns the path to one WorldPop age and sex structure raster

    The raster is downloaded if it is not already present. Rasters cover the whole
    country and are stored by ISO3 code, so subregions share the same files.
    """
    assert region.iso3 is not None
    url = worldpop_age_sex_url(region.iso3, year, sex, age)
    output_path = (
        get_path("sources", region.iso3, "worldpop", "age_sex_structure")
        / url.split("/")[-1]
    )
    if output_path.exists():
        return output_path
    logger.info("Downloading %s", url)
    if not download_file(url, output_path):
        raise requests.ConnectionError(f"Failed to download {url=}")
    return output_path


@register_fetch("worldpop.age_sex_structure")
def worldpop_age_sex_structure_fetch(
    region: BaseRegion,
    date: str,
    sex: str | None = None,
    age: str | int | None = None,
) -> Literal[False]:
    """Fetch WorldPop age and sex structure rasters

    Downloads one raster for each selected sex and age group. The ``sex`` and
    ``age`` parameters are the same as for processing.
    """
    year = worldpop_age_sex_year(date)
    for s in parse_sexes(sex):
        for a in parse_age_groups(age):
            get_worldpop_age_sex_path(region, year, s, a)
    return False


@register_process("worldpop.age_sex_structure")
def worldpop_age_sex_structure_process(
    region: AdministrativeLevel,
    date: str,
    sex: str | None = None,
    age: str | int | None = None,
) -> xr.DataArray:
    """Population count by sex and age group, summed over administrative regions

    Parameters
    ----------
    region : geoglue.AdministrativeLevel
        Administrative level to aggregate to
    date : str
        Year, from 2015 to 2030
    sex : str | None
        Comma separated sexes to process: ``f`` (female), ``m`` (male) and
        ``t`` (both). If None, processes ``f,m``. From the CLI: ``sex=t``
    age : str | int | None
        Comma separated ages to process, see :func:`parse_age_groups`. Each
        item is an age such as ``3``, an inclusive age range such as ``0-5``,
        or an age range with no upper limit such as ``65+``. The fewest age
        groups that cover the requested ages are processed, so ``0-5``
        processes <1, 1-4 and 5-9. If None, processes all age groups. From
        the CLI: ``age=0-5,65+``

    Returns
    -------
    xr.DataArray
        Dimensions are (time, region, sex, age). The ``sex`` coordinate is
        ``female``, ``male`` or ``both``. The ``age`` coordinate is the lower
        bound of the age group in years, and the ``age_group`` coordinate gives
        a label such as ``5-9`` or ``90+``. When ``sex`` is ``f,m``, the
        population for both sexes is the sum over ``sex``; do not sum over
        ``sex`` when ``both`` is included.
    """
    year = worldpop_age_sex_year(date)
    sexes = parse_sexes(sex)
    ages = parse_age_groups(age)
    geom = region.read()
    by_sex = []
    for s in sexes:
        by_age = []
        for a in ages:
            # Read one raster at a time to keep memory use low for large countries
            population = read_geotiff(get_worldpop_age_sex_path(region, year, s, a))
            by_age.append(zonalstats(population, geom, "sum").reset_coords(drop=True))
        by_sex.append(xr.concat(by_age, dim="age"))

    da = (
        xr.concat(by_sex, dim="sex")
        .assign_coords(
            sex=[WORLDPOP_SEXES[s] for s in sexes],
            age=ages,
            age_group=("age", [age_group_label(a) for a in ages]),
        )
        .transpose("region", "sex", "age")
        .astype("float32")
    )
    da["sex"].attrs["long_name"] = "sex"
    da["age"].attrs.update({"long_name": "lower bound of age group", "units": "years"})
    da["age_group"].attrs["long_name"] = "age group in years"

    name, cfattrs = get_name_cfattrs("worldpop.age_sex_structure")
    da.attrs.update(cfattrs)
    da.attrs["DART_region"] = str(region)
    return da.rename(name).expand_dims(time=[pd.Timestamp(str(year))])
