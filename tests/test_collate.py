"""
Tests for collate functions in collate.py
"""

from dart_pipeline.types import URLCollection
from dart_pipeline.collate import gadm_data


def test_gadm_data():
    assert gadm_data("VNM") == URLCollection(
        "https://geodata.ucdavis.edu/gadm/gadm4.1",
        [
            f"shp/gadm41_VNM_shp.zip",
            f"gpkg/gadm41_VNM.gpkg",
            f"json/gadm41_VNM_0.json",
            f"json/gadm41_VNM_1.json.zip",
            f"json/gadm41_VNM_2.json.zip",
            f"json/gadm41_VNM_3.json.zip",
        ],
        relative_path="VNM",
    )
