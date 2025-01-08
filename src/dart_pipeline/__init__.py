"""Main code for DART Pipeline."""
from pathlib import Path
from typing import cast
import argparse
import inspect
import logging
import os
import textwrap

from .types import DataFile, URLCollection
from .constants import (
    BASE_DIR,
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
    unpack_file,
    update_or_create_output
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
    """Get files for a source."""
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
    unpack = 'unpack' in kwargs

    if not (path := DATA_PATH / source).exists():
        path.mkdir(parents=True, exist_ok=True)
    links = SOURCES[source](**kwargs)
    source_fmt = f"\033[1m{source}\033[0m"
    links = links if isinstance(links, list) else [links]
    if isinstance(links[0], DataFile):
        print(f"-- {source_fmt} fetches data directly, nothing to do")
        return
    if not links[0]:
        print(f"-- {source_fmt} downloads data directly, nothing to do")
        return
    links = cast(list[URLCollection], links)
    auth = get_credentials(source) if source in REQUIRES_AUTH else None
    # If only one link is being downloaded, reduce the list of links to one
    if only_one:
        links = map(only_one_from_collection, links)
    # Iterate over the links
    for coll in links:
        if not coll.missing_files(DATA_PATH / source) and not update:
            print(f"✅ SKIP {source_fmt} {coll}")
            # If the file(s) have already been downloaded, they might not have
            # been unpacked
            if unpack:
                for file in coll.files:
                    to_unpack = path / coll.relative_path / Path(file).name
                    print(f'• UNPACKING {to_unpack}', end='\r')
                    unpack_file(to_unpack, same_folder=True)
                    print(f'✅ UNPACKED {to_unpack}')
            continue
        msg = f"GET {source_fmt} {coll}"
        print(f" •  {msg}", end="\r")
        success = download_files(coll, path, auth=auth, unpack=unpack)
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
    """Process a data source according to inputs from the command line."""
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
    base_path = BASE_DIR / DEFAULT_OUTPUT_ROOT / source
    result = result if isinstance(result, list) else [result]
    for df, filename in result:
        out = base_path / filename
        if not out.parent.exists():
            out.parent.mkdir(parents=True)
        update_or_create_output(df, out)
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
    """
    Parse the parameters that have been passed to the script via the CLI.

    Including a parameter such as `admin_level=0` on the command line will
    result in it being parsed as a dictionary: `{'admin_level': '0'}`.

    Command line arguments whose values get converted into integers:

    - `year`

    Shorthands that are recognised as standing for longer arguments:

    - `a` (for `admin_level`)
    - `3` (for `iso3`)
    - `d` (for `partial_date`)
    - `l` (for `logging_level`)
    """
    out = {}
    for param in params:
        if '=' in param:
            k, v = param.split("=")
            key = k.replace("-", "_")
            v = int(v) if key in INTEGER_PARAMS else v
            out[key] = v
        else:
            # This is a Boolean
            out[param] = True
    # Replace shorthand kwargs
    if 'a' in out:
        out['admin_level'] = out.pop('a')
    if '3' in out:
        out['iso3'] = out.pop('3')
    if 'd' in out:
        out['partial_date'] = out.pop('d')
    if 'l' in out:
        out['logging_level'] = out.pop('l')

    return out


def main():
    parser = argparse.ArgumentParser(description="DART Pipeline Operations")

    subparsers = parser.add_subparsers(dest="command")
    _ = subparsers.add_parser("list", help="List sources and processes")
    check_parser = subparsers.add_parser(
        "check", help="Check files exist for a given source"
    )
    check_parser.add_argument("source", help="Source to check files for")

    get_parser = subparsers.add_parser(
        "get",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Get files for a source",
        epilog=textwrap.dedent("""
        keyword arguments:
          3=, iso3=        an ISO 3166-1 alpha-3 country code

        Boolean flags:
          unpack           the downloaded files will be unpacked if they are
                           zipped
        """)
    )
    get_parser.add_argument("source", help="source to get files for")
    get_parser.add_argument("--update", help="update cached files")
    get_parser.add_argument(
        "-1", "--only-one", help="get only one file", action="store_true"
    )
    get_parser.add_argument(
        "-p",
        "--process",
        help="if the source can be directly processed, process immediately",
        action="store_true",
    )

    process_parser = subparsers.add_parser(
        "process",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Process a source",
        usage="dart-pipeline process [-h] source [**kwargs]",
        description="Process a source with optional keyword arguments.",
        epilog=textwrap.dedent("""
        keyword arguments:
          3=, iso3=          an ISO 3166-1 alpha-3 country code
          a=, admin_level=   an administrative level for the given country;
                             must be one of the following: 0, 1, 2 or 3.
          d=, partial_date=  either a year in YYYY format, a month in YYYY-MM
                             format or a day in YYYY-MM-DD format.
          l=, logging_level= minimum logging level to display, defaults to
                             'WARNING'
        Boolean flags:
          plots              plots will be created
        """)
    )
    process_parser.add_argument("source", help="source to process")

    args, unknownargs = parser.parse_known_args()
    kwargs = parse_params(unknownargs)

    if 'logging_level' in kwargs:
        match kwargs['logging_level']:
            case 'DEBUG':
                logging.basicConfig(level=logging.DEBUG)
            case 'INFO':
                logging.basicConfig(level=logging.INFO)
            case 'ERROR':
                logging.basicConfig(level=logging.ERROR)
            case 'CRITICAL':
                logging.basicConfig(level=logging.CRITICAL)
        del kwargs['logging_level']

    match args.command:
        case "list":
            print("\n".join(list_all()))
        case "get":
            get(
                args.source, args.only_one, args.update, args.process, **kwargs
            )
        case "check":
            check(args.source, args.only_one)
        case "process":
            process_cli(args.source, **kwargs)
        case _:
            print(bold_brackets(USAGE))


if __name__ == "__main__":
    main()
