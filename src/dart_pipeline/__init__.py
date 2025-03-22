"""Main code for DART Pipeline."""

import os
import logging
import argparse
import importlib

from .metrics import get, process as process_metric, print_metrics, gather_metrics
from .paths import get_path

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

USAGE = f"""[DART] – [D]engue [A]dvanced [R]eadiness [T]ools pipeline

The aim of this project is to develop a scalable and reproducible
pipeline for the joint analysis of epidemiological, climate, and
behavioural data to anticipate and predict dengue outbreaks.

The [dart-pipeline] command line tool downloads and processes data for
ingestion into a database. It has the following subcommands

     [get]    Gets data from a particular source. Sources may need
            additional parameters to be set.
    [list]    Lists sources and processors of the data
 [process]    Processes data downloaded by a particular source

To see detailed help on any of these, run
    uv run dart-pipeline <subcommand> --help

[EXAMPLES]

To get geospatial GADM data for Vietnam:
    uv run dart-pipeline get gadm VNM

By default, data will be processed if a processor with the same name
exists, otherwise you can run the process subcommand:
    uv run dart-pipeline process gadm VNM

To find out if a processor or a getter requires parameters, run without
parameters:
    uv run dart-pipeline process gadm
    ❗ geospatial/gadm missing required parameters {"iso3", "admin_level"}

[PATHS]

       Default sources path = {get_path("sources")}
Default process output path = {get_path("output")}

Files will be downloaded into the sources path and process functions will write
to the output path and a scratch path for intermediate files.
"""

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

    The second parameter, if present, is *always* interpreted as a year or
    partial date, such as 2020 or 2020-01 to represent January 2020.
    """
    out = {}
    if not params:
        return {}
    iso3 = params.pop(0)
    out: dict[str, str | int | bool] = {"iso3": iso3}
    if params:
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
        case _:
            print(USAGE.replace("[", "\033[1m").replace("]", "\033[0m"))


if __name__ == "__main__":
    main()
