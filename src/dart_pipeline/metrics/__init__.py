import re
import json
import inspect
import logging
import textwrap
from pathlib import Path
from typing import TypedDict, Unpack, cast

from geoglue.region import ZonedBaseRegion
from geoglue.zonalstats import zonalstats
import xarray as xr
import pandas as pd
import geoglue.util

from ..paths import get_path
from ..util import (
    abort,
    download_files,
    logfmt,
    determine_netcdf_filename,
)
from ..types import DataFile, URLCollection, InvalidCounts

logger = logging.getLogger(__name__)

METRICS = {}
FETCHERS = {}
PROCESSORS = {}
MULTIPLE_YEAR_PROCESSORS: dict[str, bool] = {}

# Do not automatically process these metrics
SKIP_AUTO_PROCESS = ["era5", "ecmwf.forecast"]

METRICS_USAGE_COMMON_TEXT = """
For metrics with a 'partOf' attribute, the metric is calculated by invoking
dart-pipeline (get|process) on the 'partOf' value rather than the metric value.
This is usually done for efficiency reasons when it is faster to process
multiple metrics at once.

To fetch data for a particular metric, run

.. code-block::

   uv run dart-pipeline get metric REGION-admin YYYY param=value

where ``param`` and ``value`` refer to optional parameters and their values
that are passed directly to the appropriate function. The first parameter
specifies the region code or ISO3 code of the country and the administrative level (1, 2, or
3) to aggregate data to. The second parameter is usually the year (can also be
the date) for which to download and process data. If a processor exists, it is
invoked automatically, or can be manually invoked by:

.. code-block::

   uv run dart-pipeline process metric REGION-admin YYYY param=value

Note that weather data from the ``era5`` source is not automatically processed
as fetching takes a long time. For this case run the get and process steps
separately.
"""


def assert_metrics_and_sources_registered():
    for m in list(FETCHERS.keys()) + list(PROCESSORS.keys()):
        parts = m.split(".")
        source = parts[0]
        if source not in METRICS:
            raise ValueError(f"Source {source=} not found in metric registry")

        if len(parts) > 1:
            metric_without_source_prefix = ".".join(parts[1:])
            if metric_without_source_prefix not in METRICS[source]["metrics"]:
                raise ValueError(
                    f"Metric {metric_without_source_prefix!r} must be registered as "
                    f"part of {source=} using register_metrics()"
                )


def gather_metrics() -> list[str]:
    root = Path(__file__).parent
    paths = [
        str(p.relative_to(root))
        .replace(".py", "")
        .replace("__init__", "")
        .replace("/", ".")
        .replace("\\", ".")
        .removesuffix(".")
        for p in root.rglob("*.py")
    ]
    paths = [p for p in paths if p != ""]
    return paths


class MetricInfo(TypedDict, total=False):
    url: str
    long_name: str
    depends: list[str]
    units: str
    citation: str
    license: str
    license_text: str
    standard_name: str
    short_name: str
    short_name_max: str
    short_name_min: str
    valid_min: int | float
    valid_max: int | float
    statistics: list[str]
    part_of: str
    cell_methods: str


class CFAttributes(TypedDict, total=False):
    long_name: str
    standard_name: str
    units: str
    valid_min: int | float
    valid_max: int | float
    cell_methods: str


class SourceInfo(TypedDict, total=False):
    description: str
    license_text: str
    auth_url: str
    metrics: dict[str, MetricInfo]


def register_metrics(source: str, **kwargs: Unpack[SourceInfo]):
    if source in METRICS:
        METRICS[source]["metrics"].update(kwargs.get("metrics", {}))
    else:
        METRICS[source] = kwargs


def get_cell_methods(agg: str, dim_name: str = "time") -> str:
    "Returns CF-compliant cell_methods from temporal aggregation"
    if agg == "weekly_sum":
        return f"{dim_name}: sum (interval: 1 week)"
    if agg.startswith("weekly_"):
        raise ValueError(
            "Weekly aggregation for instantaneous variables is only done at the 'collate' step"
        )
    if not agg.startswith("daily_"):
        raise ValueError(f"Unsupported aggregation {agg=}")
    agg_method = agg.removeprefix("daily_")
    return f"{dim_name}: {agg_method} (interval: 1 day)"


def get_metric_info(metric: str) -> MetricInfo:
    "Returns metric information"
    source, _, metric = metric.partition(".")
    if source not in METRICS:
        raise ValueError(f"Source {source=} not found in metric registry")
    agg_pattern = r"\b\.(weekly|daily)_(sum|min|max|mean)\b_?"
    match = re.search(agg_pattern, metric)
    agg_str = match.group(0)[1:] if match else None
    metric_stem = re.sub(agg_pattern, "", metric)
    if metric_stem not in METRICS[source]["metrics"]:
        raise ValueError(f"No metric {metric_stem!r} found in {source=}")
    info = METRICS[source]["metrics"][metric_stem]
    if agg_str:
        info["cell_methods"] = get_cell_methods(agg_str)
    return info


def subset_cfattrs(info: MetricInfo) -> CFAttributes:
    "Returns CF-compliant attribute subset of MetricInfo"
    out: CFAttributes = {
        "long_name": info.get("long_name", ""),
        "standard_name": info.get("standard_name", ""),
        "units": info.get("units", "1"),
    }
    if info.get("valid_min") is not None:
        out["valid_min"] = info.get("valid_min")  # type: ignore
    if info.get("valid_max") is not None:
        out["valid_max"] = info.get("valid_max")  # type: ignore
    if info.get("cell_methods"):
        out["cell_methods"] = info.get("cell_methods", "")
    return out


def register_fetch(metric: str):
    def decorator(func):
        FETCHERS[metric] = func
        return func

    return decorator


def register_process(metric: str, multiple_years: bool = False):
    def decorator(func):
        PROCESSORS[metric] = func
        MULTIPLE_YEAR_PROCESSORS[metric] = multiple_years
        return func

    return decorator


def get(
    metric: str,
    skip_process=False,
    **kwargs,
):
    """Get files for a source."""
    if metric not in FETCHERS:
        abort("metric not found:", metric)
    link_getter = FETCHERS[metric]
    non_default_params = {
        p.name
        for p in inspect.signature(link_getter).parameters.values()
        if p.default is p.empty
    }
    if missing_params := non_default_params - set(kwargs):
        abort(metric, f"missing required parameters {missing_params}")

    region = kwargs.get("region")
    region_name = region.name if region is not None else "WLD"
    path = get_path("sources", region_name)
    links = FETCHERS[metric](**kwargs)
    links = links if isinstance(links, list) else [links]
    if isinstance(links[0], (DataFile, Path)) or not links[0]:
        logger.info(f"Metric {metric} downloads data directly, nothing to do")
    if isinstance(links[0], URLCollection):
        links = cast(list[URLCollection], links)
        for coll in links:
            logger.info("Fetching %s [%s]: %r", metric, region, coll)
            coll.relative_path = metric.replace(".", "/")
            success = download_files(coll, path, auth=None, unpack=True)
            n_ok = sum(success)
            if n_ok == len(success):
                logger.info("Fetch %s [%s] OK", metric, region)
            elif n_ok > 0:
                logger.warning(f"Fetch partial {metric} [{n_ok}/{len(success)} OK]")
            else:
                logger.error("Fetch %s [%s] failed", metric, region)
    if not skip_process and metric in PROCESSORS and metric not in SKIP_AUTO_PROCESS:
        process(metric, **kwargs)


def print_path(p: Path) -> str:
    if " " in str(p):
        return '"' + str(p) + '"'
    return str(p)


def print_paths(ps: list[Path]) -> str:
    return "\n\t" + "\n\t".join(map(print_path, ps))


def blockfmt(s: str, indent: int) -> str:
    return textwrap.indent(textwrap.dedent(s).strip(), " " * indent)


def determine_date_signifier(s: pd.Series) -> str:
    "Determine date signifier in output file from a date timeseries"
    tstr = sorted(set(s.astype(str)))
    years = [x.split("-")[0] for x in tstr]
    if len(years) == 1:  # one year only:
        return years[0]
    else:
        return f"{min(tstr)}_{max(tstr)}"


def process(metric: str, **kwargs) -> list[Path]:
    """Process a data source according to inputs from the command line."""
    logger.info("Processing %s %s", metric, logfmt(kwargs))
    source = metric.split(".")[0]
    if source not in METRICS:
        raise ValueError(
            "Metric first part (before .) refers to a metric source that must be registered using register_metrics()"
        )
    if metric not in PROCESSORS:
        abort("metric not found:", metric)
    processor = PROCESSORS[metric]
    non_default_params = {
        p.name
        for p in inspect.signature(processor).parameters.values()
        if p.default is p.empty
    }
    if missing_params := non_default_params - set(kwargs):
        abort(metric, f"missing required parameters {missing_params}")
    res: pd.DataFrame | xr.Dataset | list[Path] = processor(**kwargs)
    if isinstance(res, list) and all(isinstance(r, Path) for r in res):
        logger.info("output %s %s", metric, print_paths(res))
        return res  # nothing to do, processor has already written data
    assert not isinstance(res, list)
    match res:
        case pd.DataFrame():
            _region = res.DART_REGION.unique()[0]
            data_metric = res.metric.unique()[0]
            admin = int(res.attrs["admin"])
            assert admin in [1, 2, 3], f"Invalid administrative level {admin=}"
            if not data_metric.startswith(metric):
                raise ValueError(
                    f"Metric returned by processor {data_metric=} is not in the class of {metric=}"
                )
            date_signifier = determine_date_signifier(res.date)
            outfile = (
                get_path("output", _region, source)
                / f"{_region}-{admin}-{date_signifier}-{data_metric}.parquet"
            )
            res.to_parquet(outfile, index=False)
            logger.info("output %s %s", metric, print_path(outfile))
            return [outfile]
        case xr.Dataset() | xr.DataArray():
            region_name = kwargs["region"].name
            outfile = get_path(
                "output", region_name, source
            ) / determine_netcdf_filename(metric, **kwargs)
            res.to_netcdf(outfile)
            logger.info("output %s %s", metric, print_path(outfile))
            return [outfile]
        case _:
            raise ValueError(f"Unsupported result type {res=}")


def find_metrics(
    metric: str, region: str | None = None, date: int | str | None = None
) -> list[Path]:
    "Lists output files that match metric"
    main = region if region else "global"
    source = metric.split(".")[0]
    glob = f"{main}-{date}-{metric}.*" if date else f"{main}-{metric}.*"
    return list(get_path("output", main, source).glob(glob))


def show_path(m: Path):
    match m.suffix:
        case ".nc":
            print(xr.open_dataset(m))
        case ".parquet":
            print(pd.read_parquet(m))
        case ".csv":
            print(pd.read_csv(m))
        case ".json":
            data = json.loads(m.read_text())
            print(json.dumps(data, sort_keys=True, indent=2))
        case _:
            print(m)


def find_metric(
    metric: str, region: str | None = None, date: str | None = None
) -> pd.DataFrame | xr.Dataset | dict | Path:
    "Reads in metric if only one match found"

    ms = find_metrics(metric, region, date)
    if len(ms) > 1:
        raise ValueError(f"No unique data file found for {metric=}, {region=}, {date=}")
    m = ms[0]
    match m.suffix:
        case ".nc":
            return xr.open_dataset(m)
        case ".parquet":
            return pd.read_parquet(m)
        case ".csv":
            return pd.read_csv(m)
        case ".json":
            return json.loads(m.read_text())
        case _:
            return m


def get_gamma_params(
    region: ZonedBaseRegion, index: str, yrange: tuple[int, int] | None = None
) -> xr.Dataset:
    info = "See https://dart-pipeline.readthedocs.io/en/latest/standardised_indices.html for more information"
    root = get_path("output", region.name, "era5")
    if yrange:
        ystart, yend = yrange
        output_file = root / f"{region.name}-{ystart}-{yend}-era5.{index}.gamma.nc"
        if not output_file.exists():
            raise FileNotFoundError(
                f"Could not find gamma parameters at: {output_file}\n\t{info}"
            )
    else:
        # find the first matching gamma parameters file
        output_files = list(root.glob(f"{region.name}-*-era5.{index}.gamma.nc"))
        if not output_files:
            raise FileNotFoundError(
                f"Could not find gamma parameters at: {root}\n\t{info}"
            )
        if len(output_files) > 1:
            logger.warning(
                f"Multiple gamma parameters found for {region.name} {index=}, selecting the first one"
            )
        output_file = output_files[0]

    ds = xr.open_dataset(output_file)
    return ds


def get_invalid_counts(da: xr.DataArray) -> InvalidCounts:
    "Validates a xarray.DataArray according to enclosed valid_min, valid_max attributes"

    valid_min = da.attrs.get("valid_min", None)
    valid_max = da.attrs.get("valid_max", None)

    below_min = (da < valid_min).sum().item() if valid_min is not None else None
    above_max = (da > valid_max).sum().item() if valid_max is not None else None
    nan_count = da.isnull().sum().item()

    return InvalidCounts(below_min, above_max, nan_count, da.size)


def print_metrics(filter_by: str | None = None):
    print("\033[1mMETRICS\033[0m")
    print(METRICS_USAGE_COMMON_TEXT.replace(".. code-block::\n\n", ""))
    filtered_metrics = [
        f"{src}.{mt}" for src in METRICS for mt in METRICS[src]["metrics"]
    ]
    if filter_by:
        filtered_metrics = [m for m in filtered_metrics if filter_by in m]
    filtered_sources = (
        METRICS.keys()
        if filter_by is None
        else set(m.split(".")[0] for m in filtered_metrics)
    )
    for s in filtered_sources:
        print()
        source = METRICS[s]
        print(f"\033[36m\033[1m{s}\033[0m - \033[36m{source['description']}\033[0m")
        if urls := source.get("url"):
            if isinstance(urls, str):
                urls = [urls]
            print("\n".join(f"  URL: {u}" for u in urls))
        if not source.get("redistribution_allowed", True):
            print("  \033[2mRedistribution not allowed\033[0m")
        if source.get("auth_url"):
            print(f"  \033[3mAuthentication required, see {source['auth_url']}\033[0m")
        if source.get("resolution"):
            print("  Resolution:", source["resolution"])

        # show license
        if source.get("license"):
            print(f"  License: {source['license']}")
        if source.get("license_text"):
            print("  License:")
            print(textwrap.indent(source["license_text"], "    "))
        if source.get("license_url"):
            print("  License-URL:", source["license_url"])

        print()
        matched_metrics = [
            ".".join(m.split(".")[1:]) for m in filtered_metrics if m.split(".")[0] == s
        ]
        for m_name in matched_metrics:
            metric: MetricInfo = source["metrics"][m_name]
            print(f"  \033[1m{s}.{m_name}\033[0m")
            print(f"    {metric.get('long_name', m_name)} [{metric.get('units', '1')}]")
            if part_of := metric.get("part_of"):
                print("    \033[3mpart of\033[0m:", part_of)
            if metric.get("url"):
                print("    URL:", metric.get("url"))
            if metric.get("license"):
                print("    License:", metric.get("license"))
            if metric.get("license_text"):
                print("    License:")
                print(blockfmt(metric.get("license_text", ""), 6))
            if metric.get("citation"):
                print("    Citation:")
                print(blockfmt(metric.get("citation", ""), 6))


def print_metrics_rst(filter_by: str | None = None):
    print("*******\nMetrics\n*******")
    print(METRICS_USAGE_COMMON_TEXT)

    filtered_metrics = [
        f"{src}.{mt}" for src in METRICS for mt in METRICS[src]["metrics"]
    ]
    if filter_by:
        filtered_metrics = [m for m in filtered_metrics if filter_by in m]
    filtered_sources = (
        METRICS.keys()
        if filter_by is None
        else set(m.split(".")[0] for m in filtered_metrics)
    )
    for s in filtered_sources:
        source = METRICS[s]
        print()
        print(source["description"], "-", f"``{s}``")
        print("===========================================================")
        if urls := source.get("url"):
            if isinstance(urls, str):
                urls = [urls]
            print(":URL:", "; ".join(u for u in urls))
        if not source.get("redistribution_allowed", True):
            print(":Redistribution: Redistribution not allowed**")
        if source.get("auth_url"):
            print(f":Authentication: Authentication required, see {source['auth_url']}")
        if source.get("resolution"):
            print(":Resolution:", source["resolution"])

        # show license
        if source.get("license"):
            print(f":License: {source['license']}")
        if source.get("license_text"):
            print(":License:")
            print(textwrap.indent(source["license_text"], "    "))
        if source.get("license_url"):
            print(":License-URL:", source["license_url"])

        matched_metrics = [
            ".".join(m.split(".")[1:]) for m in filtered_metrics if m.split(".")[0] == s
        ]
        for m_name in matched_metrics:
            metric = source["metrics"][m_name]
            print(f"\n{s}.{m_name}")
            partOf = f", *partOf* {metric['part_of']}" if metric.get("part_of") else ""
            print(f"    {metric['description']} [{metric['unit']}]{partOf}\n")
            if metric.get("url"):
                print("    :URL:", metric["url"])
            if metric.get("license"):
                print("    :License:", metric["license"])
            if metric.get("license_text"):
                print("    :License:")
                print(blockfmt(metric["license_text"], 6))
            if metric.get("citation"):
                print("    :Citation:")
                print(blockfmt(metric["citation"], 6))
            if metric.get("resolution"):
                print("    :Resolution:", metric["resolution"])


def get_name_cfattrs(metric: str) -> tuple[str, CFAttributes]:
    metric_info = get_metric_info(metric)
    # Default short name is derived from metric name without the source
    short_name = metric_info.get("short_name") or "_".join(metric.split(".")[1:])
    cell_methods = metric_info.get("cell_methods", "")
    if "min" in cell_methods:
        name = metric_info.get("short_name_min") or ("mn" + short_name)
    elif "max" in cell_methods:
        name = metric_info.get("short_name_max") or ("mx" + short_name)
    else:
        name = short_name
    return name, subset_cfattrs(get_metric_info(metric))


def zonal_stats(
    metric: str,
    da: xr.DataArray,
    region: geoglue.region.Region,
    operation: str = "mean(coverage_weight=area_spherical_km2)",
    weights: xr.DataArray | None = None,
    fix_array: bool = False,
) -> pd.DataFrame:
    """Return zonal statistics for a particular DataArray as a DataFrame

    This is a wrapper around geoglue.zonalstats.zonalstats to add metadata attributes
    such as metric, unit and region name to the dataframe.

    Parameters
    ----------
    metric : str
        Name of metric
    da : xr.DataArray
        xarray DataArray to perform zonal statistics on. Must have
        'latitude', 'longitude' and a time coordinate
    region : geoglue.region.Region
        Region for which to calculate zonal statistics
    operation : str
        Zonal statistics operation. For a full list of operations, see
        https://isciences.github.io/exactextract/operations.html. Default
        operation is to calculate the mean with a spherical area coverage weight.
    weights : xr.DataArray | None
        Optional, if specified, uses the specified raster to perform weighted
        zonal statistics.
    include_cols : list[str] | None
        Optional, if specified, only includes these columns. If not specified,
        returns all columns except the geometry column
    fix_array : bool
        Whether to perform pre-processing steps, such as sorting longitude, latitude
        and setting CF-compliant attributes. These should not be required
        when processing downloaded weather data which should already be in
        compliant format. Optional, default=False

    Returns
    -------
    pd.DataFrame
        The DataFrame specified by the geometry in the `region` parameter, one
        additional column, `value` containing the zonal statistic for the corresponding geometry.

    See Also
    --------
    zonal_stats_xarray
        Version of this function that returns a xarray DataArray
    """
    if fix_array:
        da = geoglue.util.sort_lonlat(da)  # type: ignore
        geoglue.util.set_lonlat_attrs(da)  # type: ignore
    geom = region.read()
    da = zonalstats(da, geom, operation, weights)
    da["region"] = region.name
    units = get_metric_info(metric).get("units", "1")
    da["unit"] = units
    da["metric"] = metric
    df = da.to_dataframe()
    return df


def zonal_stats_xarray(
    metric: str,
    da: xr.DataArray,
    region: geoglue.AdministrativeLevel,
    operation: str = "mean(coverage_weight=area_spherical_km2)",
    weights: xr.DataArray | None = None,
    fix_array: bool = False,
) -> xr.DataArray:
    """Return zonal statistics for a particular DataArray as another xarray DataArray

    This is a wrapper around geoglue.zonalstats.zonalstats to add CF-compliant
    metadata attributes derived from the metric name

    Parameters
    ----------
    metric : str
        Name of metric
    da : xr.DataArray
        xarray DataArray to perform zonal statistics on. Must have
        'latitude', 'longitude' and a time coordinate
    region : geoglue.AdministrativeLevel
        Administrative level for which to calculate zonal statistics
    operation : str
        Zonal statistics operation. For a full list of operations, see
        https://isciences.github.io/exactextract/operations.html. Default
        operation is to calculate the mean with a spherical area coverage weight.
    weights : xr.DataArray | None
        Optional, if specified, uses the specified raster to perform weighted
        zonal statistics.
    include_cols : list[str] | None
        Optional, if specified, only includes these columns. If not specified,
        returns all columns except the geometry column
    fix_array : bool
        Whether to perform pre-processing steps, such as sorting longitude, latitude
        and setting CF-compliant attributes. These should not be required
        when processing downloaded weather data which should already be in
        compliant format. Optional, default=False

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

    See Also
    --------
    zonal_stats
        Version of this function that returns a pandas DataFrame
    """
    if fix_array:
        da = geoglue.util.sort_lonlat(da)  # type: ignore
        geoglue.util.set_lonlat_attrs(da)  # type: ignore
    geom = region.read()
    za = zonalstats(da, geom, operation, weights)
    x, y = za.shape
    call = f"zonal_stats({metric!r}, {da.name!r}, region, {operation=}, {weights=})"
    if x == 0 or y == 0:
        raise ValueError(f"Zero dimension DataArray created from {call}")
    name, cfattrs = get_name_cfattrs(metric)
    za.attrs.update(cfattrs)
    za.attrs["DART_zonal_stats"] = call
    za.attrs["DART_region"] = str(region)
    return za.rename(name)


def convert_parquet_netcdf(infile: Path, region_col: str) -> Path:
    """Converts existing parquet file to netCDF

    This function converts parquet to netCDF files, adding CF-compliant
    attributes. This is useful in the transition to netCDF output format
    for DART-Pipeline.
    """
    output_folder = infile.parent
    output_name = infile.stem + ".nc"
    if infile.suffix != ".parquet":
        raise ValueError("Can only convert parquet file to netCDF, got %s", infile)
    df = pd.read_parquet(infile)
    if region_col not in df.columns:
        raise ValueError(f"Could not find {region_col=}")
    metric = infile.stem.split("-")[3]
    df = df.rename(columns={region_col: "region"})
    pivoted = df[["region", "date", "value"]].pivot(
        index="date", columns="region", values="value"
    )
    name, cfattrs = get_name_cfattrs(metric)
    da = xr.DataArray(
        data=pivoted.values,
        coords={"date": pivoted.index, "region": pivoted.columns},
        dims=["date", "region"],
    )
    da.coords["region"].attrs["original_name"] = region_col
    da.attrs.update(cfattrs)
    da = da.rename(name)
    da.to_netcdf(output_folder / output_name)
    return output_folder / output_name
