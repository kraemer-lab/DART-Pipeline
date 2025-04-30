"""
Tests for collate functions in collate.py
"""

from dart_pipeline.types import URLCollection
from dart_pipeline.collate import (
    gadm_data,
    worldpop_pop_density_data,
)


def test_gadm_data():
    assert gadm_data("VNM") == URLCollection(
        "https://geodata.ucdavis.edu/gadm/gadm4.1",
        [
            "shp/gadm41_VNM_shp.zip",
            "gpkg/gadm41_VNM.gpkg",
            "json/gadm41_VNM_0.json",
            "json/gadm41_VNM_1.json.zip",
            "json/gadm41_VNM_2.json.zip",
            "json/gadm41_VNM_3.json.zip",
        ],
        relative_path="VNM",
    )


def test_worldpop_pop_density_data():
    assert worldpop_pop_density_data("VNM") == URLCollection(
        "https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km_UNadj/2020/VNM",
        ["vnm_pd_2020_1km_UNadj_ASCII_XYZ.zip", "vnm_pd_2020_1km_UNadj.tif"],
        relative_path="VNM",
    )
