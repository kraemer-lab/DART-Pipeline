import logging
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import numpy as np
import pytest
import requests
import xarray as xr
from geoglue.region import AdministrativeLevel, BaseCountry
from geoglue.types import Bbox
from shapely.geometry import box

from dart_pipeline.metrics import process
from dart_pipeline.metrics.worldpop import (
    WORLDPOP_AGE_GROUPS,
    age_group_end,
    age_group_label,
    age_groups_covering,
    get_worldpop,
    get_worlpop_cache,
    parse_age_groups,
    parse_sexes,
    worldpop_age_sex_structure_fetch,
    worldpop_age_sex_structure_process,
    worldpop_age_sex_url,
    worldpop_age_sex_year,
    worldpop_pop_count_fetch,
    worldpop_pop_count_process,
)

NGA = BaseCountry(
    "NGA", "http://gadm.org", Bbox(minx=2, miny=4, maxx=15, maxy=14), "NGA"
)
GBR = BaseCountry(
    "GBR", "http://gadm.org", Bbox(minx=-9, miny=49, maxx=2, maxy=61), "GBR"
)


@pytest.fixture
def mock_raster():
    mock_raster = MagicMock(spec=xr.DataArray)
    mock_raster.nbytes = 1024
    return mock_raster


@pytest.fixture
def mock_path(tmp_path):
    return tmp_path / "output.tif"


@pytest.mark.parametrize(
    "year,dataset,expected_url",
    [
        (
            2000,
            None,
            "https://data.worldpop.org/GIS/Population/Global_2000_2020_1km_UNadj/2000/GBR/gbr_ppp_2000_1km_Aggregated_UNadj.tif",
        ),
        (
            2020,
            "default",
            "https://data.worldpop.org/GIS/Population/Global_2000_2020_1km_UNadj/2020/GBR/gbr_ppp_2020_1km_Aggregated_UNadj.tif",
        ),
        (
            2016,
            "future",
            "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2024B/2016/GBR/v1/1km_ua/unconstrained/gbr_pop_2016_UC_1km_R2024B_UA_v1.tif",
        ),
        (
            2029,
            None,
            "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2024B/2029/GBR/v1/1km_ua/unconstrained/gbr_pop_2029_UC_1km_R2024B_UA_v1.tif",
        ),
    ],
)
def test_get_worldpop_success(year, dataset, expected_url, mock_path, mock_raster):
    with (
        patch("dart_pipeline.metrics.worldpop.get_path", return_value=mock_path.parent),
        patch(
            "dart_pipeline.metrics.worldpop.download_file", return_value=True
        ) as mock_download,
        patch("dart_pipeline.metrics.worldpop.read_geotiff", return_value=mock_raster),
    ):
        get_worldpop(GBR, year, dataset)
        mock_download.assert_called_once()
        called_url = mock_download.call_args[0][0]
        assert called_url == expected_url, (
            f"Expected URL {expected_url}, got {called_url}"
        )


def test_get_worldpop_future_warns_on_past_year(mock_raster, mock_path):
    with (
        patch("dart_pipeline.metrics.worldpop.get_path", return_value=mock_path.parent),
        patch("dart_pipeline.metrics.worldpop.download_file", return_value=True),
        patch("dart_pipeline.metrics.worldpop.read_geotiff", return_value=mock_raster),
        warnings.catch_warnings(record=True) as w,
    ):
        warnings.simplefilter("always")
        get_worldpop(NGA, 2015, "future")
        assert any(
            "consider using actual data using dataset='default'" in str(warn.message)
            for warn in w
        )


def test_get_worldpop_dataset_none_and_year_invalid():
    with pytest.raises(ValueError, match="No pre-defined dataset found for"):
        get_worldpop(NGA, 2040, None)


def test_get_worldpop_invalid_year_range():
    with pytest.raises(
        ValueError,
        match="Worldpop population data for dataset='default' is only available",
    ):
        get_worldpop(NGA, 1990, "default")


@pytest.mark.parametrize(
    "iso3,year,sex,age,expected_url",
    [
        (
            "VNM",
            2015,
            "f",
            0,
            "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2015/VNM/v1/1km_ua/constrained/vnm_f_00_2015_CN_1km_R2025A_UA_v1.tif",
        ),
        (
            "vnm",
            2030,
            "m",
            90,
            "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2030/VNM/v1/1km_ua/constrained/vnm_m_90_2030_CN_1km_R2025A_UA_v1.tif",
        ),
        (
            "SGP",
            2020,
            "m",
            1,
            "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2020/SGP/v1/1km_ua/constrained/sgp_m_01_2020_CN_1km_R2025A_UA_v1.tif",
        ),
        (
            "VNM",
            2015,
            "t",
            5,
            "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2015/VNM/v1/1km_ua/constrained/vnm_t_05_2015_CN_1km_R2025A_UA_v1.tif",
        ),
    ],
)
def test_worldpop_age_sex_url(iso3, year, sex, age, expected_url):
    assert worldpop_age_sex_url(iso3, year, sex, age) == expected_url


@pytest.mark.parametrize("sex,age", [("x", 0), ("both", 0), ("f", 2), ("m", 95)])
def test_worldpop_age_sex_url_invalid(sex, age):
    with pytest.raises(ValueError, match="Invalid"):
        worldpop_age_sex_url("VNM", 2020, sex, age)


def test_worldpop_age_groups():
    assert len(WORLDPOP_AGE_GROUPS) == 20
    assert WORLDPOP_AGE_GROUPS[:4] == [0, 1, 5, 10]
    assert WORLDPOP_AGE_GROUPS[-2:] == [85, 90]


@pytest.mark.parametrize(
    "age,label", [(0, "<1"), (1, "1-4"), (5, "5-9"), (85, "85-89"), (90, "90+")]
)
def test_age_group_label(age, label):
    assert age_group_label(age) == label


@pytest.mark.parametrize(
    "sex,expected",
    [
        (None, ["f", "m"]),
        ("t", ["t"]),
        ("t,f", ["f", "t"]),
        ("M, f,m", ["f", "m"]),
        ("f,m,t", ["f", "m", "t"]),
    ],
)
def test_parse_sexes(sex, expected):
    assert parse_sexes(sex) == expected


@pytest.mark.parametrize("sex", ["x", "f,x", "", ",", "female", True])
def test_parse_sexes_invalid(sex):
    with pytest.raises(ValueError, match="such as sex=f,m or sex=t"):
        parse_sexes(sex)


@pytest.mark.parametrize(
    "age,expected",
    [
        (None, WORLDPOP_AGE_GROUPS),
        (5, [5]),
        (0, [0]),
        ("10,0,5,5", [0, 5, 10]),
        ("00, 01", [0, 1]),
        # a single age selects the age group with that age
        ("3", [1]),
        ("4", [1]),
        ("95", [90]),
        # ranges select the fewest age groups that cover them
        ("0-0", [0]),
        ("0-4", [0, 1]),
        ("0-5", [0, 1, 5]),
        ("0-9", [0, 1, 5]),
        ("2-11", [1, 5, 10]),
        ("5-5", [5]),
        ("15-49", [15, 20, 25, 30, 35, 40, 45]),
        ("15-50", [15, 20, 25, 30, 35, 40, 45, 50]),
        ("100-120", [90]),
        ("0 - 5", [0, 1, 5]),
        ("80+", [80, 85, 90]),
        ("83+", [80, 85, 90]),
        ("90+", [90]),
        ("0+", WORLDPOP_AGE_GROUPS),
        ("0-5,65+", [0, 1, 5, 65, 70, 75, 80, 85, 90]),
        ("0-9,5-14", [0, 1, 5, 10]),
    ],
)
def test_parse_age_groups(age, expected):
    assert parse_age_groups(age) == expected


@pytest.mark.parametrize(
    "age,message",
    [
        ("10-4", r"range '10-4' ends before it starts"),
        ("a", r"could not read 'a'"),
        ("-5", r"could not read '-5'"),
        ("5-", r"could not read '5-'"),
        ("1.5", r"could not read '1.5'"),
        (True, r"could not read 'True'"),
        ("", r"e\.g\. age=0-5,65\+$"),
        (",", r"e\.g\. age=0-5,65\+$"),
    ],
)
def test_parse_age_groups_invalid(age, message):
    with pytest.raises(ValueError, match=message):
        parse_age_groups(age)


@pytest.mark.parametrize(
    "age,message",
    [
        ("0-5", "age=0-5: WorldPop age groups <1, 1-4, 5-9 cover ages 0-9"),
        ("2-11", "age=2-11: WorldPop age groups 1-4, 5-9, 10-14 cover ages 1-14"),
        ("83+", "age=83+: WorldPop age groups 80-84, 85-89, 90+ cover ages 80+"),
    ],
)
def test_parse_age_groups_warns_when_covering_more(caplog, age, message):
    with caplog.at_level(logging.WARNING, logger="dart_pipeline.metrics.worldpop"):
        parse_age_groups(age)
    assert [r.getMessage() for r in caplog.records] == [
        message + ", more than requested"
    ]


@pytest.mark.parametrize("age", ["0-4", "0-0", "15-49", "65+", "3", "0,1,5", None])
def test_parse_age_groups_no_warning_when_exact(caplog, age):
    with caplog.at_level(logging.WARNING, logger="dart_pipeline.metrics.worldpop"):
        parse_age_groups(age)
    assert caplog.records == []


@pytest.mark.parametrize(
    "start,end,expected",
    [(0, 5, [0, 1, 5]), (4, 5, [1, 5]), (0, 0, [0]), (89, None, [85, 90])],
)
def test_age_groups_covering(start, end, expected):
    assert age_groups_covering(start, end) == expected


@pytest.mark.parametrize("age,end", [(0, 0), (1, 4), (5, 9), (85, 89), (90, None)])
def test_age_group_end(age, end):
    assert age_group_end(age) == end


@pytest.mark.parametrize("date", ["2014", "2031"])
def test_worldpop_age_sex_year_out_of_range(date):
    with pytest.raises(ValueError, match="only available for 2015-2030"):
        worldpop_age_sex_year(date)


def test_worldpop_age_sex_year_not_a_year():
    with pytest.raises(ValueError, match="need a single year"):
        worldpop_age_sex_year("2020-2021")


def fake_download(url: str, path: Path) -> bool:
    path.write_bytes(b"")
    return True


def test_worldpop_age_sex_structure_fetch(tmp_path, admin_region):
    # file already present is not downloaded again
    (tmp_path / "vnm_f_00_2020_CN_1km_R2025A_UA_v1.tif").write_bytes(b"")
    with (
        patch("dart_pipeline.metrics.worldpop.get_path", return_value=tmp_path),
        patch(
            "dart_pipeline.metrics.worldpop.download_file", side_effect=fake_download
        ) as mock_download,
    ):
        assert worldpop_age_sex_structure_fetch(admin_region, "2020") is False
    urls = [c.args[0] for c in mock_download.call_args_list]
    assert len(urls) == 2 * len(WORLDPOP_AGE_GROUPS) - 1
    assert not any("vnm_f_00_2020" in u for u in urls)
    assert any(
        u.endswith(
            "/2020/VNM/v1/1km_ua/constrained/vnm_m_90_2020_CN_1km_R2025A_UA_v1.tif"
        )
        for u in urls
    )
    assert len(list(tmp_path.glob("*.tif"))) == 2 * len(WORLDPOP_AGE_GROUPS)


def test_worldpop_age_sex_structure_fetch_selection(tmp_path, admin_region):
    with (
        patch("dart_pipeline.metrics.worldpop.get_path", return_value=tmp_path),
        patch(
            "dart_pipeline.metrics.worldpop.download_file", side_effect=fake_download
        ) as mock_download,
    ):
        # ages 0 to 5 need the <1, 1-4 and 5-9 age groups, and nothing else
        worldpop_age_sex_structure_fetch(admin_region, "2020", sex="t", age="0-5")
    files = [c.args[0].split("/")[-1] for c in mock_download.call_args_list]
    assert files == [
        "vnm_t_00_2020_CN_1km_R2025A_UA_v1.tif",
        "vnm_t_01_2020_CN_1km_R2025A_UA_v1.tif",
        "vnm_t_05_2020_CN_1km_R2025A_UA_v1.tif",
    ]


def test_worldpop_age_sex_structure_fetch_failure(tmp_path, admin_region):
    with (
        patch("dart_pipeline.metrics.worldpop.get_path", return_value=tmp_path),
        patch("dart_pipeline.metrics.worldpop.download_file", return_value=False),
        pytest.raises(requests.ConnectionError, match="vnm_f_00_2020"),
    ):
        worldpop_age_sex_structure_fetch(admin_region, "2020")


def synthetic_population(path: Path) -> xr.DataArray:
    "4x4 raster where each cell has population 10 * sex_factor + age_group_index"
    _, sex, age, *_ = path.name.split("_")
    value = 10 * {"f": 1, "m": 2, "t": 3}[sex] + WORLDPOP_AGE_GROUPS.index(int(age))
    data = np.full((4, 4), value, dtype="float32")
    data[0, 0] = np.nan  # outside settlement, constrained rasters have nodata
    return xr.DataArray(
        data,
        coords={
            "latitude": [3.5, 2.5, 1.5, 0.5],
            "longitude": [0.5, 1.5, 2.5, 3.5],
            "band": 1,
            "spatial_ref": 0,
        },
        dims=("latitude", "longitude"),
        name="band_data",
    )


@pytest.fixture
def admin_region():
    region = MagicMock(spec=AdministrativeLevel)
    region.name = "HCM"
    region.iso3 = "VNM"
    region.admin = 1
    region.__str__.return_value = "HCM-1"
    region.read.return_value = gpd.GeoDataFrame(
        {"GID_1": ["A", "B"]},
        geometry=[box(0, 0, 2, 4), box(2, 0, 4, 4)],
        crs="EPSG:4326",
    ).set_index("GID_1")
    return region


@pytest.fixture
def mock_age_sex_rasters():
    with (
        patch(
            "dart_pipeline.metrics.worldpop.get_worldpop_age_sex_path",
            side_effect=lambda region, year, sex, age: Path(
                f"{region.iso3.lower()}_{sex}_{age:02d}_{year}_CN_1km_R2025A_UA_v1.tif"
            ),
        ) as mock_path,
        patch(
            "dart_pipeline.metrics.worldpop.read_geotiff",
            side_effect=synthetic_population,
        ),
    ):
        yield mock_path


def test_worldpop_age_sex_structure_process(admin_region, mock_age_sex_rasters):
    da = worldpop_age_sex_structure_process(admin_region, "2020")
    assert da.name == "age_sex_structure"
    assert da.dims == ("time", "region", "sex", "age")
    assert da.shape == (1, 2, 2, len(WORLDPOP_AGE_GROUPS))
    assert str(da.time.values[0]).startswith("2020-01-01")
    assert list(da.region.values) == ["A", "B"]
    assert list(da.sex.values) == ["female", "male"]
    assert list(da.age.values) == WORLDPOP_AGE_GROUPS
    assert da.age_group.sel(age=90).item() == "90+"
    assert da.attrs["units"] == "1"
    assert da.attrs["valid_min"] == 0
    assert da.attrs["DART_region"] == "HCM-1"
    # region A has 7 valid cells (one nodata), region B has 8
    assert da.sel(region="A", sex="female", age=0).item() == 7 * 10
    assert da.sel(region="B", sex="male", age=90).item() == 8 * (20 + 19)
    # rasters are looked up by the region's ISO3 code
    assert all(c.args[0].iso3 == "VNM" for c in mock_age_sex_rasters.call_args_list)
    assert mock_age_sex_rasters.call_count == 2 * len(WORLDPOP_AGE_GROUPS)


def test_worldpop_age_sex_structure_logs_year(
    admin_region, mock_age_sex_rasters, caplog
):
    with caplog.at_level(logging.INFO, logger="dart_pipeline.metrics.worldpop"):
        worldpop_age_sex_structure_fetch(admin_region, "2021", sex="t", age="0-5")
        worldpop_age_sex_structure_process(admin_region, "2021", sex="t", age="0-5")
    fetched, processed = [
        r.getMessage() for r in caplog.records if r.levelno == logging.INFO
    ]
    assert fetched == (
        "Fetching worldpop.age_sex_structure for VNM, year 2021: "
        "3 rasters (sex=t, 3 age groups)"
    )
    assert processed == (
        "Processing worldpop.age_sex_structure for HCM-1, year 2021 "
        "(sex=t, 3 age groups)"
    )


@pytest.fixture
def empty_worldpop_cache():
    "get_worldpop() is cached, so a cache hit would skip the download"
    get_worlpop_cache.clear()
    yield
    get_worlpop_cache.clear()


def test_worldpop_pop_count_logs_year(
    tmp_path, admin_region, empty_worldpop_cache, caplog
):
    population = xr.DataArray(
        np.ones((4, 4), dtype="float32"),
        coords={
            "latitude": [3.5, 2.5, 1.5, 0.5],
            "longitude": [0.5, 1.5, 2.5, 3.5],
            "band": 1,
            "spatial_ref": 0,
        },
        dims=("latitude", "longitude"),
        name="band_data",
    )
    with (
        patch("dart_pipeline.metrics.worldpop.get_path", return_value=tmp_path),
        patch(
            "dart_pipeline.metrics.worldpop.download_file", side_effect=fake_download
        ) as mock_download,
        patch("dart_pipeline.metrics.worldpop.read_geotiff", return_value=population),
        caplog.at_level(logging.INFO, logger="dart_pipeline.metrics.worldpop"),
    ):
        worldpop_pop_count_fetch(admin_region, "2019")
        worldpop_pop_count_process(admin_region, "2019")
    # processing reuses the raster that was just fetched
    mock_download.assert_called_once()
    assert [r.getMessage() for r in caplog.records] == [
        "Fetching worldpop.pop_count for VNM, year 2019",
        (
            "Downloading https://data.worldpop.org/GIS/Population/"
            "Global_2000_2020_1km_UNadj/2019/VNM/vnm_ppp_2019_1km_Aggregated_UNadj.tif"
        ),
        "Processing worldpop.pop_count for HCM-1, year 2019",
    ]


def test_worldpop_age_sex_structure_netcdf(
    tmp_path, admin_region, mock_age_sex_rasters
):
    with patch("dart_pipeline.metrics.get_path", return_value=tmp_path):
        (outfile,) = process(
            "worldpop.age_sex_structure", region=admin_region, date="2020"
        )
    assert outfile.name == "HCM-1-2020-worldpop.age_sex_structure.nc"
    ds = xr.open_dataset(outfile)
    da = ds["age_sex_structure"]
    assert da.dims == ("time", "region", "sex", "age")
    assert list(ds.age_group.values[:3]) == ["<1", "1-4", "5-9"]
    assert ds.age.attrs["units"] == "years"
    assert da.sum("sex").sel(region="B", age=5).item() == 8 * (10 + 2 + 20 + 2)


def test_get_worldpop_custom_template_is_relative_to_population(mock_path, mock_raster):
    with (
        patch("dart_pipeline.metrics.worldpop.get_path", return_value=mock_path.parent),
        patch(
            "dart_pipeline.metrics.worldpop.download_file", return_value=True
        ) as mock_download,
        patch("dart_pipeline.metrics.worldpop.read_geotiff", return_value=mock_raster),
    ):
        get_worldpop(GBR, 2019, "Custom/$year/${iso3_lower}_$iso3.tif")
    assert (
        mock_download.call_args[0][0]
        == "https://data.worldpop.org/GIS/Population/Custom/2019/gbr_GBR.tif"
    )


def test_worldpop_age_sex_structure_process_selection(
    admin_region, mock_age_sex_rasters
):
    da = worldpop_age_sex_structure_process(
        admin_region, "2020", sex="t,f", age="90+,0"
    )
    assert da.shape == (1, 2, 2, 2)
    assert list(da.sex.values) == ["female", "both"]
    assert list(da.age.values) == [0, 90]
    assert list(da.age_group.values) == ["<1", "90+"]
    assert da.sel(region="B", sex="both", age=90).item() == 8 * (30 + 19)
    assert da.sel(region="A", sex="female", age=0).item() == 7 * 10
    requested = [c.args[2:] for c in mock_age_sex_rasters.call_args_list]
    assert requested == [("f", 0), ("f", 90), ("t", 0), ("t", 90)]


def test_worldpop_age_sex_structure_netcdf_selection(
    tmp_path, admin_region, mock_age_sex_rasters
):
    # the CLI passes age=5 as an int
    with patch("dart_pipeline.metrics.get_path", return_value=tmp_path):
        (outfile,) = process(
            "worldpop.age_sex_structure",
            region=admin_region,
            date="2020",
            sex="t",
            age=5,
        )
    assert outfile.name == "HCM-1-2020-worldpop.age_sex_structure.t.5.nc"
    da = xr.open_dataset(outfile)["age_sex_structure"]
    assert list(da.sex.values) == ["both"]
    assert da.sel(region="B", sex="both", age=5).item() == 8 * (30 + 2)


def test_worldpop_age_sex_structure_netcdf_range(
    tmp_path, admin_region, mock_age_sex_rasters
):
    with patch("dart_pipeline.metrics.get_path", return_value=tmp_path):
        (outfile,) = process(
            "worldpop.age_sex_structure", region=admin_region, date="2020", age="0-4"
        )
    assert outfile.name == "HCM-1-2020-worldpop.age_sex_structure.0-4.nc"
    da = xr.open_dataset(outfile)["age_sex_structure"]
    assert list(da.age_group.values) == ["<1", "1-4"]
    # under 5, both sexes, region B: f (10 + 0) + f (10 + 1) + m (20 + 0) + m (20 + 1)
    assert da.sel(region="B").sum(["sex", "age"]).item() == 8 * (10 + 11 + 20 + 21)
