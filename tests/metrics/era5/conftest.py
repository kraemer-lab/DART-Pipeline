import pytest

from geoglue.region import Country
from geoglue.types import Bbox


@pytest.fixture
def singapore_region():
    return Country(
        "SGP",
        "https://gadm.org",
        Bbox(maxy=2, minx=103, miny=1, maxx=105),
        "SGP",
        "+08:00",
        {1: "/path/to/SGP.shp"},
        {1: "GID_1"},
    )
