"""
Main code for DART Pipeline
"""

import os
import inspect
import argparse
from pathlib import Path
from typing import cast

from .types import DataFile, URLCollection
from .constants import (
    DEFAULT_SOURCES_ROOT,
    DEFAULT_OUTPUT_ROOT,
    MSG_PROCESS,
    MSG_SOURCE,
    INTEGER_PARAMS,
)
from .collate import SOURCES, REQUIRES_AUTH
from .process import PROCESSORS
from .util import (
    abort,
    bold_brackets,
    download_files,
    get_credentials,
    only_one_from_collection,
    output_path,
)

DATA_PATH = Path(os.getenv("DART_PIPELINE_SOURCES_PATH", DEFAULT_SOURCES_ROOT))

USAGE = f"""[DART] – [D]engue [A]dvanced [R]eadiness [T]ools pipeline

The aim of this project is to develop a scalable and reproducible
pipeline for the joint analysis of epidemiological, climate, and
behavioural data to anticipate and predict dengue outbreaks.

The [dart-pipeline] command line tool downloads and processes data for
ingestion into a database. It has the following subcommands

   [check]    Checks that data from a particular source exists
     [get]    Gets data from a particular source. Sources may need
            additional parameters to be set.
    [list]    Lists sources and processors of the data
 [process]    Processes data downloaded by a particular source

To see detailed help on any of these, run
    uv run dart-pipeline <subcommand> --help

[EXAMPLES]

To get geospatial GADM data for Vietnam:
    uv run dart-pipeline get geospatial/gadm iso3=VNM

If a processor with the same name exists, you can also process the data
at the same time by adding a [-p] flag:
    uv run dart-pipeline get geospatial/gadm iso3=VNM -p

To find out if a processor or a getter requires parameters, run without:
    uv run dart-pipeline process geospatial/gadm
    ❗ geospatial/gadm missing required parameters {'iso3', 'admin_level'}

[PATHS]

       Default sources path = {DEFAULT_SOURCES_ROOT}
Default process output path = {DEFAULT_OUTPUT_ROOT}

Files will be downloaded into the sources path and process
functions will write to the output path.
"""


def list_all() -> list[str]:
    "Lists all sources and processors"
    return [f"{MSG_SOURCE} {source}" for source in sorted(SOURCES)] + [
        f"{MSG_PROCESS} {process}" for process in sorted(PROCESSORS)
    ]


def get(
    source: str,
    only_one: bool = True,
    update: bool = False,
    process: bool = False,
    **kwargs,
):
    "Get files for a source"
    if source not in SOURCES:
        abort("source not found:", source)
    link_getter = SOURCES[source]
    non_default_params = {
        p.name
        for p in inspect.signature(link_getter).parameters.values()
        if p.default is p.empty
    }
    if missing_params := non_default_params - set(kwargs):
        abort(source, f"missing required parameters {missing_params}")

    if not (path := DATA_PATH / source).exists():
        path.mkdir(parents=True, exist_ok=True)
    links = SOURCES[source](**kwargs)
    source_fmt = f"\033[1m{source}\033[0m"
    links = links if isinstance(links, list) else [links]
    if isinstance(links[0], DataFile):
        print(f"-- {source_fmt} fetches data directly, nothing to do")
        return
    links = cast(list[URLCollection], links)
    auth = get_credentials(source) if source in REQUIRES_AUTH else None
    for coll in links if not only_one else map(only_one_from_collection, links):
        if not coll.missing_files(DATA_PATH / source) and not update:
            print(f"✅ SKIP {source_fmt} {coll}")
            continue
        msg = f" GET {source_fmt} {coll}"
        print(f" • {msg}", end="\r")
        success = download_files(coll, path, auth=auth)
        n_ok = sum(success)
        if n_ok == len(success):
            print(f"✅ {msg}")
        elif n_ok > 0:
            print(f"🟡 {msg} [{n_ok}/{len(success)} OK]")
        else:
            print(f"❌ {msg}")
    if process and source in PROCESSORS:
        process_cli(source, **kwargs)


def process_cli(source: str, **kwargs):
    if source not in PROCESSORS:
        abort("source not found:", source)
    print(f" • PROC \033[1m{source}\033[0m ...", end="\r")
    processor = PROCESSORS[source]
    non_default_params = {
        p.name
        for p in inspect.signature(processor).parameters.values()
        if p.default is p.empty
    }
    if missing_params := non_default_params - set(kwargs):
        abort(source, f"missing required parameters {missing_params}")
    result = processor(**kwargs)
    base_path = output_path(source)
    result = result if isinstance(result, list) else [result]
    for df, filename in result:
        out = base_path / filename
        if not out.parent.exists():
            out.parent.mkdir(parents=True)
        df.to_csv(out, index=False)
        print(f"✅ PROC \033[1m{source}\033[0m {out}")


def check(source: str, only_one: bool = True, **kwargs):
    "Check files exist for a source"
    links = SOURCES[source](**kwargs)
    links = links if isinstance(links, list) else [links]
    if isinstance(links, list) and isinstance(links[0], DataFile):
        print("-- \033[1m{source}\033[0m directly returns data, checking not supported")
    links = cast(list[URLCollection], links)
    for coll in links if not only_one else map(only_one_from_collection, links):
        missing = coll.missing_files(DATA_PATH / source)
        indicator = "✅" if not missing else "❌"
        print(f"{indicator} PROC \033[1m{source}\033[0m {coll}")
        if missing:
            print("\n".join("   missing " + str(p) for p in missing))


def parse_params(params: list[str]) -> dict[str, str | int]:
    out = {}
    for param in params:
        k, v = param.split("=")
        key = k.replace("-", "_")
        v = v if key not in INTEGER_PARAMS else int(v)
        out[key] = v
    return out


def main():
    parser = argparse.ArgumentParser(description="DART Pipeline Operations")

    subparsers = parser.add_subparsers(dest="command")
    _ = subparsers.add_parser("list", help="List sources and processes")
    check_parser = subparsers.add_parser(
        "check", help="Check files exist for a given source"
    )
    check_parser.add_argument("source", help="Source to check files for")

    get_parser = subparsers.add_parser("get", help="Get files for a source")
    get_parser.add_argument("source", help="Source to get files for")
    get_parser.add_argument("--update", help="Update cached files")
    get_parser.add_argument(
        "-1", "--only-one", help="Get only one file", action="store_true"
    )
    get_parser.add_argument(
        "-p",
        "--process",
        help="If the source can be directly processed, process immediately",
        action="store_true",
    )

    process_parser = subparsers.add_parser("process", help="Process a source")
    process_parser.add_argument("source", help="Source to process")

    args, unknownargs = parser.parse_known_args()
    kwargs = parse_params(unknownargs)
    match args.command:
        case "list":
            print("\n".join(list_all()))
        case "get":
            get(args.source, args.only_one, args.update, args.process, **kwargs)
        case "check":
            check(args.source, args.only_one)
        case "process":
            process_cli(args.source, **kwargs)
        case _:
            print(bold_brackets(USAGE))


if __name__ == "__main__":
    main()
