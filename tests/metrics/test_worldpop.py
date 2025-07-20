import warnings
from unittest.mock import patch, MagicMock

from geoglue import MemoryRaster
import pytest

from dart_pipeline.metrics.worldpop import get_worldpop


@pytest.fixture
def mock_raster():
    return MagicMock(spec=MemoryRaster)


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
        patch("dart_pipeline.paths.get_path", return_value=mock_path.parent),
        patch(
            "dart_pipeline.metrics.worldpop.download_file", return_value=True
        ) as mock_download,
        patch("geoglue.MemoryRaster.read", return_value=mock_raster),
    ):
        get_worldpop("GBR", year, dataset)
        mock_download.assert_called_once()
        called_url = mock_download.call_args[0][0]
        assert (
            called_url == expected_url
        ), f"Expected URL {expected_url}, got {called_url}"


def test_get_worldpop_future_warns_on_past_year(mock_raster, mock_path):
    with (
        patch("dart_pipeline.paths.get_path", return_value=mock_path.parent),
        patch("dart_pipeline.metrics.worldpop.download_file", return_value=True),
        patch("geoglue.MemoryRaster.read", return_value=mock_raster),
        warnings.catch_warnings(record=True) as w,
    ):
        warnings.simplefilter("always")
        get_worldpop("ETH", 2015, "future")
        assert any(
            "consider using actual data using dataset='default'" in str(warn.message)
            for warn in w
        )


def test_get_worldpop_dataset_none_and_year_invalid():
    with pytest.raises(ValueError, match="No pre-defined dataset found for"):
        get_worldpop("NGA", 2040, None)


def test_get_worldpop_invalid_year_range():
    with pytest.raises(
        ValueError,
        match="Worldpop population data for dataset='default' is only available",
    ):
        get_worldpop("NGA", 1990, "default")
