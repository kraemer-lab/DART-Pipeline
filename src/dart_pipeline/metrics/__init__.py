import inspect
import logging
import textwrap
from pathlib import Path
from typing import TypedDict, Unpack, cast

import pandas as pd

from ..paths import get_path
from ..util import abort, unpack_file, download_files
from ..types import DataFile, URLCollection

METRICS = {}
FETCHERS = {}
PROCESSORS = {}

# Do not automatically process these metrics
SKIP_AUTO_PROCESS = ["era5"]


def gather_metrics() -> list[str]:
    return [
        f.stem
        for f in Path(__file__).parent.glob("*")
        if (f.is_dir() and (f / "__init__.py").exists())
        or (f.suffix == ".py" and f.name != "__init__.py")
    ]


class MetricInfo(TypedDict, total=False):
    description: str
    depends: list[str]
    unit: str
    range: tuple[int, int] | tuple[float, float]
    statistics: list[str]


class SourceInfo(TypedDict, total=False):
    description: str
    license_text: str
    auth_url: str
    metrics: dict[str, MetricInfo]


def register_metrics(source: str, **kwargs: Unpack[SourceInfo]):
    METRICS[source] = kwargs


def register_fetch(metric: str):
    if metric.split(".")[0] not in METRICS:
        raise ValueError(
            "Metric first part (before .) refers to a metric source that must be registered using register_metrics()"
        )
    if "." in metric:
        source, metric = metric.split(".")[:2]
        if metric in METRICS[source]["metrics"]:
            raise ValueError("Metric must be registered using register_metrics()")

    def decorator(func):
        FETCHERS[metric] = func

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
    source = metric.split(".")[0]
    link_getter = FETCHERS[metric]
    non_default_params = {
        p.name
        for p in inspect.signature(link_getter).parameters.values()
        if p.default is p.empty
    }
    if missing_params := non_default_params - set(kwargs):
        abort(metric, f"missing required parameters {missing_params}")

    path = get_path("sources", source)
    links = FETCHERS[metric](**kwargs)
    source_fmt = f"\033[1m{source}\033[0m"
    links = links if isinstance(links, list) else [links]
    if isinstance(links[0], DataFile):
        logging.info(f"GET {source_fmt} fetches data directly, nothing to do")
        return
    if not links[0]:
        logging.info(f"GET {source_fmt} downloads data directly, nothing to do")
        return
    if isinstance(links[0], URLCollection):
        links = cast(list[URLCollection], links)
        for coll in links:
            if not coll.missing_files(path) and not update:
                logging.info(f"skip {source_fmt} {coll}")
                # unpack files
                for file in coll.files:
                    to_unpack = path / coll.relative_path / Path(file).name
                    unpack_file(to_unpack, same_folder=True)
                    logging.info(f"unpacked {to_unpack}")
            msg = f"GET {source_fmt} {coll}"
            success = download_files(coll, path, auth=None, unpack=True)
            n_ok = sum(success)
            if n_ok == len(success):
                logging.info(f"{msg}")
            elif n_ok > 0:
                logging.warning(f"partial {msg} [{n_ok}/{len(success)} OK]")
            else:
                logging.error(msg)
    if not skip_process and metric in PROCESSORS and metric not in SKIP_AUTO_PROCESS:
        process(metric, **kwargs)


def print_path(p: Path) -> str:
    if " " in str(p):
        return '"' + str(p) + '"'
    return str(p)


def print_paths(ps: list[Path]) -> str:
    return " ".join(map(print_path, ps))


def process(metric: str, **kwargs) -> list[Path]:
    """Process a data source according to inputs from the command line."""
    logging.info("processing %s", metric)
    source = metric.split(".")[0]
    if source not in PROCESSORS:
        abort("source not found:", source)
    processor = PROCESSORS[metric]
    non_default_params = {
        p.name
        for p in inspect.signature(processor).parameters.values()
        if p.default is p.empty
    }
    if missing_params := non_default_params - set(kwargs):
        abort(source, f"missing required parameters {missing_params}")
    res: pd.DataFrame | list[Path] = processor(**kwargs)
    if isinstance(res, list) and all(isinstance(r, Path) for r in res):
        return res  # nothing to do, processor has already written data
    assert isinstance(res, pd.DataFrame)
    iso3 = res.ISO3.unique()[0]
    data_metric = res.metric.unique()[0]
    admin = res.attrs["admin"]
    if data_metric != metric:
        raise ValueError(
            f"Metric returned by processor {data_metric=} differs from requested {metric=}"
        )
    outfile = get_path("output", source) / f"{iso3}-{admin}-{metric}.parquet"
    res.to_parquet(outfile, index=False)
    return [outfile]


def print_metrics(filter_by: str | None = None):
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
        urls = source.get("url", "Not available")
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
            metric = source["metrics"][m_name]
            print(f"  \033[1m{s}.{m_name}\033[0m")
            print(f"    {metric['description']} [{metric['unit']}]")
            if metric.get("resolution"):
                print("    Resolution:", metric["resolution"])
