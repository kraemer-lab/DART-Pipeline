"""
Tests for collate functions in collate.py
"""

from pathlib import Path

import requests_mock

from dart_pipeline.types import URLCollection
from dart_pipeline.collate import (
    gadm_data,
    relative_wealth_index,
    chirps_rainfall_data,
    meta_pop_density_data,
    worldpop_pop_count_data,
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


def test_relative_wealth_index():
    web_snapshot = Path("tests/webarchive/relative-wealth-index.html")
    base_url = "https://data.humdata.org/dataset/relative-wealth-index"
    with requests_mock.Mocker() as m:
        m.get(
            base_url,
            text=web_snapshot.read_text(),
        )
        assert relative_wealth_index("VNM") == URLCollection(
            "https://data.humdata.org",
            [
                "/dataset/76f2a2ea-ba50-40f5-b79c-db95d668b843/resource/06d29bc0-5a4c-4be0-be1a-c546a9be540c/download/vnm_relative_wealth_index.csv"
            ],
        )


def test_chirps_rainfall_data():
    base_url = "https://data.chc.ucsb.edu"
    assert chirps_rainfall_data('2020') == [
        URLCollection(
            f"{base_url}/products/CHIRPS-2.0/global_annual/tifs",
            ["chirps-v2.0.2020.tif"],
            relative_path="global_annual",
        )
    ]

    base_url = 'https://data.chc.ucsb.edu'
    assert chirps_rainfall_data('2020-01') == [
        URLCollection(
            f'{base_url}/products/CHIRPS-2.0/global_annual/tifs',
            ['chirps-v2.0.2020.tif'],
            relative_path='global_annual',
        ),
        URLCollection(
            f"{base_url}/products/CHIRPS-2.0/global_monthly/tifs",
            ['chirps-v2.0.2020.01.tif.gz'],
            relative_path='global_monthly/2020',
        ),
        URLCollection(
            f'{base_url}/products/CHIRPS-2.0/global_daily/tifs/p05/2020',
            [f'chirps-v2.0.2020.01.{day:02d}.tif.gz' for day in range(1, 32)],
            relative_path='global_daily/2020/01',
        ),
    ]


def test_meta_pop_density_data():
    web_snapshot = Path(
        "tests/webarchive/vietnam-high-resolution-population-density-maps-demographic-estimates.html"
    )
    url = "https://data.humdata.org/dataset/vietnam-high-resolution-population-density-maps-demographic-estimates"
    dataset = "/dataset/191b04c5-3dc7-4c2a-8e00-9c0bdfdfbf9d/resource"
    with requests_mock.Mocker() as m:
        m.get(
            url,
            text=web_snapshot.read_text(),
        )
        assert meta_pop_density_data("VNM") == URLCollection(
            "https://data.humdata.org",
            files=[
                f"{dataset}/b60dab07-0a27-47f3-894d-02e9fbee6473/download/vnm_children_under_five_2020_csv.zip",
                f"{dataset}/a2ca47a3-b7d7-4a39-a605-161d4790c6f9/download/vnm_children_under_five_2020_geotiff.zip",
                f"{dataset}/77edfcc3-d037-4233-9b16-7ebb684b4752/download/vnm_elderly_60_plus_2020_csv.zip",
                f"{dataset}/61b6a396-ffcc-4710-a453-174914822164/download/vnm_elderly_60_plus_2020_geotiff.zip",
                f"{dataset}/e0d42fbb-2436-4f3e-afd1-83d1ac6314b2/download/vnm_men_2020_csv.zip",
                f"{dataset}/76990cd9-a2dc-44e6-809c-492698b090c0/download/vnm_men_2020_geotiff.zip",
                f"{dataset}/0bd525fa-3f63-447b-9afa-3d5075657ce4/download/vnm_women_2020_csv.zip",
                f"{dataset}/5bd99468-ea44-4a50-b64b-54343b182842/download/vnm_women_2020_geotiff.zip",
                f"{dataset}/5727eb4e-ce2a-4250-a9a0-2603d889ff02/download/vnm_women_of_reproductive_age_15_49_2020_csv.zip",
                f"{dataset}/855e27d7-d191-440d-936a-ecedfded238d/download/vnm_women_of_reproductive_age_15_49_2020_geotiff.zip",
                f"{dataset}/aa6dd9dd-eecd-4f3d-b6b2-fa0f0b5faa70/download/vnm_youth_15_24_2020_csv.zip",
                f"{dataset}/e36dce9f-ea1d-4933-8a37-dc5838df11f0/download/vnm_youth_15_24_2020_geotiff.zip",
                f"{dataset}/fade8620-0935-4d26-b0c6-15515dd4bf8b/download/vnm_general_2020_geotiff.zip",
                f"{dataset}/0fbf4055-7091-4041-a7ea-25f057debd7c/download/vnm_general_2020_csv.zip",
            ],
            relative_path="VNM",
        )


def test_worldpop_pop_count_data():
    assert worldpop_pop_count_data("VNM") == URLCollection(
        "https://data.worldpop.org",
        [
            "GIS/Population/Individual_countries/VNM/Vietnam_100m_Population/VNM_ppp_v2b_2020_UNadj.tif",
            "GIS/Population/Individual_countries/VNM/Vietnam_100m_Population.7z",
        ],
    )


def test_worldpop_pop_density_data():
    assert worldpop_pop_density_data("VNM") == URLCollection(
        "https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km_UNadj/2020/VNM",
        ["vnm_pd_2020_1km_UNadj_ASCII_XYZ.zip" "vnm_pd_2020_1km_UNadj.tif"],
    )
