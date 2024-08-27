"""
Main code for DART Pipeline
"""

import os
import fire
from pathlib import Path

from .collate import SOURCES, REQUIRES_AUTH
from .util import (
    download_files,
    get_credentials,
    only_one_from_collection,
    show_urlcollection,
)

DATA_PATH = Path(os.getenv("DART_PIPELINE_DATA_PATH", "data"))


def list_links(source: str, **kwargs):
    "Get links that will be downloaded for a source"
    res = SOURCES[source](**kwargs)
    for coll in (res if isinstance(res, list) else [res]):
        print(f"LIST {source} {show_urlcollection(coll, all_links=True)}")


def list_sources() -> list[str]:
    "List sources"
    return sorted(SOURCES)


def get(source: str, only_one: bool = True, **kwargs):
    "Get files for a source"
    if not (path := DATA_PATH / source).exists():
        path.mkdir(parents=True, exist_ok=True)
    links = SOURCES[source](**kwargs)
    links = links if isinstance(links, list) else [links]
    auth = get_credentials(source) if source in REQUIRES_AUTH else None
    for coll in (links if not only_one else map(only_one_from_collection, links)):
        msg = f"GET \033[1m{source}\033[0m {show_urlcollection(coll)}"
        print(f" • {msg}", end='\r')
        success = download_files(coll, path, auth=auth)
        n_ok = sum(success)
        if n_ok == len(success):
            print(f"✅ {msg}")
        elif n_ok > 0:
            print(f"🟡 {msg} [{n_ok}/{len(success)} OK]")
        else:
            print(f"❌ {msg}")



def process(source: str):
    pass


def main():
    fire.Fire(
        {
            "list": list_sources,
            "list-sources": list_sources,
            "list-links": list_links,
            "get": get,
        }
    )


if __name__ == "__main__":
    main()
