"""Worldpop population count data, aggregated to administrative levels"""

from typing import Literal

import pandas as pd
from geoglue.region import get_worldpop_1km, gadm, read_region

from ..util import iso3_admin_unpack
from ..metrics import register_metrics, register_fetch, register_process

register_metrics(
    "worldpop",
    description="WorldPop population data",
    metrics={
        "pop_count": {
            "url": "https://hub.worldpop.org/geodata/listing?id=75",
            "description": "WorldPop population count",
            "unit": "unitless",
            "license": "CC-BY-4.0",
            # We produce outputs at minimum admin1 resolution, unlikely
            # that any administrative area will have population greater than this
            "range": (0, 500_000_000),
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
    },
)


@register_fetch("worldpop.pop_count")
def worldpop_pop_count_fetch(iso3: str, date: str) -> Literal[False]:
    "Fetch worldpop count data"

    if "-" in iso3:
        iso3, _ = iso3.split("-")
    year = int(date)
    # use geoglue to do this
    _ = get_worldpop_1km(iso3, year)

    return False  # ensures that dart-pipeline get skips processing


@register_process("worldpop.pop_count")
def worldpop_pop_count_process(iso3: str, date: str) -> pd.DataFrame:
    iso3, admin = iso3_admin_unpack(iso3)
    year = int(date)
    population = get_worldpop_1km(iso3, year)
    geom = read_region(gadm(iso3, admin))
    include_cols = [c for c in geom.columns if c != "geometry"]
    df = population.zonal_stats(geom, "sum", include_cols=include_cols).rename(
        columns={"sum": "value", "GID_0": "ISO3"}
    )
    df["region"] = f"{iso3}-{admin}"
    df["metric"] = "worldpop.pop_count"
    df["unit"] = "1"
    df["date"] = year
    return df
