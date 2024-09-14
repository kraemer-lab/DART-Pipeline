"""
Main code for DART Pipeline
"""

import os
import argparse
from pathlib import Path
from typing import cast

from .types import DataFile, URLCollection, ProcessResult
from .constants import DEFAULT_SOURCES_ROOT, MSG_PROCESS, MSG_SOURCE, INTEGER_PARAMS
from .collate import SOURCES, REQUIRES_AUTH
from .process import PROCESSORS
from .util import download_files, get_credentials, only_one_from_collection, output_path

DATA_PATH = Path(os.getenv("DART_PIPELINE_SOURCES_PATH", DEFAULT_SOURCES_ROOT))


def list_all() -> list[str]:
    "Lists all sources and processors"
    return [f"{MSG_SOURCE} {source}" for source in sorted(SOURCES)] + [
        f"{MSG_PROCESS} {process}" for process in sorted(PROCESSORS)
    ]


def get(source: str, only_one: bool = True, update: bool = False, **kwargs):
    "Get files for a source"
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
            print(f"✅ SKIP {source_fmt} {coll.show()}")
            continue
        msg = f" GET {source_fmt} {coll.show()}"
        print(f" • {msg}", end="\r")
        success = download_files(coll, path, auth=auth)
        n_ok = sum(success)
        if n_ok == len(success):
            print(f"✅ {msg}")
        elif n_ok > 0:
            print(f"🟡 {msg} [{n_ok}/{len(success)} OK]")
        else:
            print(f"❌ {msg}")


def process(source: str, **kwargs):
    processor = PROCESSORS[source]
    result = processor(**kwargs)
    base_path = output_path(source)
    result = result if isinstance(result, list) else [result]
    for df, filename in result:
        out = base_path / filename
        if not out.parent.exists():
            out.parent.mkdir(parents=True)
        df.to_csv(out, index=False)
        print(f"✅ \033[1m{source}\033[0m {filename}")


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
        print(f"{indicator} \033[1m{source}\033[0m {coll.show()}")
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

    process_parser = subparsers.add_parser("process", help="Process a source")
    process_parser.add_argument("source", help="Source to process")

    args, unknownargs = parser.parse_known_args()
    kwargs = parse_params(unknownargs)
    match args.command:
        case "list":
            print("\n".join(list_all()))
        case "get":
            get(args.source, args.only_one, args.update, **kwargs)
        case "check":
            check(args.source, args.only_one)
        case "process":
            process(args.source, **kwargs)


if __name__ == "__main__":
    main()
