import re
import json
import inspect
import logging
import textwrap
from pathlib import Path
from typing import TypedDict, Unpack, cast

import xarray as xr
import pandas as pd

from ..paths import get_path
from ..util import abort, unpack_file, download_files, logfmt, determine_netcdf_filename
from ..types import DataFile, URLCollection

logger = logging.getLogger(__name__)

METRICS = {}
FETCHERS = {}
PROCESSORS = {}

# Do not automatically process these metrics
SKIP_AUTO_PROCESS = ["era5"]

METRICS_USAGE_COMMON_TEXT = """
For metrics with a 'partOf' attribute, the metric is calculated by invoking
dart-pipeline (get|process) on the 'partOf' value rather than the metric value.
This is usually done for efficiency reasons when it is faster to process
multiple metrics at once.

To fetch data for a particular metric, run

.. code-block::

   uv run dart-pipeline get metric ISO3-admin YYYY param=value

where ``param`` and ``value`` refer to optional parameters and their values
that are passed directly to the appropriate function. The first parameter
specifies the ISO3 code of the country and the administrative level (1, 2, or
3) to aggregate data to. The second parameter is usually the year (can also be
the date) for which to download and process data. If a processor exists, it is
invoked automatically, or can be manually invoked by:

.. code-block::

   uv run dart-pipeline process metric ISO3-admin YYYY param=value

Note that weather data from the ``era5`` source is not automatically processed
as fetching takes a long time. For this case run the get and process steps
separately.
"""


def gather_metrics() -> list[str]:
    root = Path(__file__).parent
    paths = [
        str(p.relative_to(root))
        .replace(".py", "")
        .replace("__init__", "")
        .replace("/", ".")
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


def register_fetch(metric: str):
    if metric.split(".")[0] not in METRICS:
        raise ValueError(
            "Metric first part (before .) refers to a metric source that must be registered using register_metrics()"
        )
    if "." in metric:
        source, metric_part = metric.split(".")[:2]
        if metric_part not in METRICS[source]["metrics"]:
            raise ValueError("Metric must be registered using register_metrics()")

    def decorator(func):
        FETCHERS[metric] = func
        return func

    return decorator


def register_process(metric: str):
    parts = metric.split(".")
    source = parts[0]
    if source not in METRICS:
        raise ValueError(
            "Metric first part (before .) refers to a metric source that must be registered using register_metrics()"
        )
    if len(parts) > 1:
        metric_without_source_prefix = ".".join(parts[1:])
        if metric_without_source_prefix not in METRICS[source]["metrics"]:
            raise ValueError(
                f"Metric {metric_without_source_prefix!r} must be registered as "
                f"part of {source=} using register_metrics()"
            )

    def decorator(func):
        PROCESSORS[metric] = func
        return func

    return decorator


def get(
    metric: str,
    update: bool = False,
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

    iso3 = kwargs.get("iso3", "WLD")  # assume entire world if no iso3 code found
    iso3 = iso3.split("-")[0]  # split out admin part
    path = get_path("sources", iso3)
    links = FETCHERS[metric](**kwargs)
    links = links if isinstance(links, list) else [links]
    if isinstance(links[0], (DataFile, Path)) or not links[0]:
        logger.info(f"Metric {metric} downloads data directly, nothing to do")
    if isinstance(links[0], URLCollection):
        links = cast(list[URLCollection], links)
        for coll in links:
            logger.info("Fetching %s [%s]: %r", metric, iso3, coll)
            coll.relative_path = metric.replace(".", "/")
            if not coll.missing_files(path) and not update:
                # unpack files
                for file in coll.files:
                    to_unpack = path / coll.relative_path / Path(file).name
                    unpack_file(to_unpack, same_folder=True)
                    logger.info("Unpacked %s", to_unpack)
            success = download_files(coll, path, auth=None, unpack=True)
            n_ok = sum(success)
            if n_ok == len(success):
                logger.info("Fetch %s [%s] OK", metric, iso3)
            elif n_ok > 0:
                logger.warning(f"Fetch partial {metric} [{n_ok}/{len(success)} OK]")
            else:
                logger.error("Fetch %s [%s] failed", metric, iso3)
    if not skip_process and metric in PROCESSORS and metric not in SKIP_AUTO_PROCESS:
        process(metric, **kwargs)


def print_path(p: Path) -> str:
    if " " in str(p):
        return '"' + str(p) + '"'
    return str(p)


def print_paths(ps: list[Path]) -> str:
    return " ".join(map(print_path, ps))


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
    if isinstance(res, pd.DataFrame):
        iso3 = res.ISO3.unique()[0]
        data_metric = res.metric.unique()[0]
        admin = int(res.attrs["admin"])
        assert admin in [1, 2, 3], f"Invalid administrative level {admin=}"
        if not data_metric.startswith(metric):
            raise ValueError(
                f"Metric returned by processor {data_metric=} is not in the class of {metric=}"
            )
        date_signifier = determine_date_signifier(res.date)
        outfile = (
            get_path("output", iso3, source)
            / f"{iso3}-{admin}-{date_signifier}-{data_metric}.parquet"
        )
        res.to_parquet(outfile, index=False)
        logger.info("output %s %s", metric, print_path(outfile))
        return [outfile]
    elif isinstance(res, xr.Dataset):
        iso3 = res.attrs.get("ISO3", kwargs["iso3"])
        metric = res.attrs.get("metric", metric)
        outfile = get_path("output", iso3, source) / determine_netcdf_filename(
            metric, **kwargs
        )
        res.to_netcdf(outfile)
        logger.info("output %s %s", metric, print_path(outfile))
        return [outfile]
    else:
        raise ValueError(f"Unsupported result type {res=}")


def find_metrics(
    metric: str, iso3: str | None = None, date: int | str | None = None
) -> list[Path]:
    "Lists output files that match metric"
    main = iso3 if iso3 else "global"
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
    metric: str, iso3: str | None = None, date: str | None = None
) -> pd.DataFrame | xr.Dataset | dict | Path:
    "Reads in metric if only one match found"

    ms = find_metrics(metric, iso3, date)
    if len(ms) > 1:
        raise ValueError(f"No unique data file found for {metric=}, {iso3=}, {date=}")
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


def validate_metric(df: pd.DataFrame) -> list[tuple[int, str]]:
    "Returns list of errors where validation failed for a particular metric file"

    errors = []
    metrics = list(df.metric.unique())
    if len(metrics) > 1:
        raise ValueError(
            f"validate_metric() does not support multiple metrics: {metrics}"
        )
    metric_name = metrics[0]
    source, _, metric = metric_name.partition(".")

    # remove aggregations
    metric = re.sub(r"(\w+)\..*(mean|min|max|sum)", r"\1", metric)
    if source not in METRICS:
        raise ValueError(f"Source not found: {source}")
    if metric not in METRICS[source]["metrics"]:
        raise ValueError(f"Metric {metric!r} not found in {source=}")

    # If NA entries present, return number of NA entries
    na_entries = df[pd.isna(df.value)]
    if not na_entries.empty:
        errors.append((len(na_entries), "NA entries"))
    metric_range = METRICS[source]["metrics"][metric].get("range")
    if metric_range is None:
        return errors
    low, high = metric_range
    df = df[~pd.isna(df.value)]
    out_of_range = df[(df.value < low) | (df.value > high)]
    if not out_of_range.empty:
        alow, ahigh = df.value.min(), df.value.max()
        errors.append(
            (
                len(out_of_range),
                f"out of range {low} -- {high}, actual range {alow:.4f} -- {ahigh:.4f}",
            )
        )
    return errors


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
