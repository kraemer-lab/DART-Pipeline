"""Main code for DART Pipeline."""

import os
import logging
import argparse
import importlib
from pathlib import Path

import pandas as pd

from .metrics import (
    get,
    process as process_metric,
    validate_metric,
    print_metrics,
    print_metrics_rst,
    gather_metrics,
    find_metrics,
    show_path,
)
from .paths import get_path
from .plots import plot_metric_data

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"

USAGE = f"""[DART] – [D]engue [A]dvanced [R]eadiness [T]ools pipeline

The aim of this project is to develop a scalable and reproducible
pipeline for the joint analysis of epidemiological, climate, and
behavioural data to anticipate and predict dengue outbreaks.

The [dart-pipeline] command line tool downloads and processes data for
ingestion into a database. It has the following subcommands

     [get]    Gets data from a particular source. Sources may need
            additional parameters to be set.
    [list]    Lists sources and processors of the data
    [plot]    Plot metric data file in the terminal
 [process]    Processes data downloaded by a particular source
    [show]    Shows data for a particular metric
[validate]    Validates metric data files

To see detailed help on any of these, run
    uv run dart-pipeline <subcommand> --help

[EXAMPLES]

To get population count data for Vietnam:
    uv run dart-pipeline get worldpop.pop_count VNM-2 2020

Note that the country code is suffixed with the administrative level,
here admin2 (district level). Aggregating to admin1 and admin3 are also
supported.

By default, data will be processed if a processor with the same name
exists, otherwise you can run the process subcommand:
    uv run dart-pipeline process worldpop.pop_count VNM-2 2020

To find out if a processor or a getter requires parameters, run without
parameters:
    $ uv run dart-pipeline process worldpop.pop_count
    2025-05-14 17:09:23,362 INFO [root] Processing worldpop.pop_count
    ❗worldpop.pop_count missing required parameters {'date', 'iso3'}

Only the iso3 and date parameters can be passed positionally. For any
other parameters, use ``param=value``.
[PATHS]

       Default sources path = {get_path("sources")}
Default process output path = {get_path("output")}

Files will be downloaded into the sources path and process functions will write
to the output path and a scratch path for intermediate files.
"""

# Convert warnings into log messages
logging.captureWarnings(True)

for metric in gather_metrics():
    importlib.import_module(f"dart_pipeline.metrics.{metric}")


def parse_params(params: list[str]) -> dict[str, str | int]:
    """
    Parse the parameters that have been passed to the script via the CLI.

    Including a parameter such as `data=hello` on the command line will result
    in it being parsed as a dictionary: `{'data': 'hello'}`.

    The first parameter, if present, is *always* interpreted as either an iso3
    code or a iso3 code paired with an admin level (1, 2, or 3), separated by a
    hyphen, e.g. VNM or VNM-2.

    The second positional parameter, if present, is interpreted as a year or
    partial date, such as 2020 or 2020-01 to represent January 2020.
    """
    out = {}
    if not params:
        return {}
    iso3 = params.pop(0)
    out: dict[str, str | int | bool] = {"iso3": iso3}
    if params and "=" not in params[0]:
        out["date"] = params.pop(0)
    for param in params:
        if "=" in param:
            k, v = param.split("=")
            key = k.replace("-", "_")
            out[key] = int(v) if v.isdigit() else v
        else:  # interpret as a boolean flag
            out[param] = True
    return out


def main():
    parser = argparse.ArgumentParser(description="DART Pipeline Operations")

    subparsers = parser.add_subparsers(dest="command")
    list_parser = subparsers.add_parser("list", help="List sources and processes")
    list_parser.add_argument("-k", help="Filter metrics by expression", dest="filter")
    list_parser.add_argument(
        "--rst", help="Output in reStructuredText format", action="store_true"
    )
    get_parser = subparsers.add_parser(
        "get",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Get files for a metric or source",
        epilog="ISO3 code must be specified, optionally with admin level such as VNM-2",
    )
    get_parser.add_argument("metric", help="source to get files for")
    get_parser.add_argument("--update", help="update cached files")
    get_parser.add_argument(
        "--skip-process",
        help="Skip immediate processing",
        action="store_true",
    )

    process_parser = subparsers.add_parser(
        "process",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Process a source",
        usage="dart-pipeline process [-h] source [**kwargs]",
        description="Process a source with optional keyword arguments.",
        epilog="""ISO3 code must be specified, optionally with admin level such as VNM-2

        Boolean flags:
          plots              plots will be created
        """,
    )
    process_parser.add_argument("metric", help="Metric to process")

    show_parser = subparsers.add_parser("show", help="Show metric data")
    show_parser.add_argument("metric", help="Metric to process")

    plot_parser = subparsers.add_parser(
        "plot", help="Plot a metric file at the terminal"
    )
    plot_parser.add_argument("files", nargs="+", help="Files to plot")
    plot_parser.add_argument(
        "--size", help="Figure size as a tuple of integers, e.g. 8,16"
    )
    plot_parser.add_argument(
        "-f",
        "--format",
        help="Output file format",
        default="console",
        choices=["png", "console"],
    )
    validate_parser = subparsers.add_parser(
        "validate", help="Validates metric data file"
    )
    validate_parser.add_argument("files", nargs="+", help="File to validate")
    validate_parser.add_argument(
        "-s", "--success", help="Show successful validations", action="store_true"
    )

    args, unknownargs = parser.parse_known_args()
    kwargs = parse_params(unknownargs)

    match os.getenv("DART_PIPELINE_LOGLEVEL", "INFO"):
        case "DEBUG":
            logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
        case "INFO":
            logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
        case "ERROR":
            logging.basicConfig(format=LOG_FORMAT, level=logging.ERROR)
        case "CRITICAL":
            logging.basicConfig(format=LOG_FORMAT, level=logging.CRITICAL)

    match args.command:
        case "list":
            if args.rst:
                print_metrics_rst(args.filter)
            else:
                print_metrics(args.filter)
        case "get":
            get(
                args.metric,
                args.update,
                skip_process=args.skip_process,
                **kwargs,
            )
        case "process":
            process_metric(args.metric, **kwargs)
        case "show":
            ms = find_metrics(args.metric, **kwargs)
            match len(ms):
                case 0:
                    print(f"No match found for {args.metric!r}, {kwargs}")
                case 1:
                    show_path(ms[0])
                case _:
                    print("\n".join(map(str, ms)))
        case "plot":
            if args.size:
                x, y = args.size.split(",")
                x, y = int(x), int(y)
                figsize = x, y
            else:
                figsize = None

            for file in args.files:
                plot_metric_data(file, figsize, args.format)
        case "validate":
            for file in args.files:
                df = pd.read_parquet(file)
                basename = Path(file).name
                errors = validate_metric(df)
                if errors:
                    N = len(df)
                    for count, err in errors:
                        print(
                            f"{basename:70s}[\033[31m{count:7d}, {count / N:6.2%}\033[0m] {err}"
                        )
                elif args.success:
                    print(f"{basename:70s}[\033[32mSUCCESS, 100.00%\033[0m]")
                else:
                    pass
        case _:
            print(USAGE.replace("[", "\033[1m").replace("]", "\033[0m"))


if __name__ == "__main__":
    main()
