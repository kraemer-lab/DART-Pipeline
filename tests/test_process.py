"""Tests for process functions in process.py."""

from io import BytesIO

from unittest.mock import patch, MagicMock

from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from dart_pipeline.process import (
    process_dengueperu,
    process_gadm_chirps_rainfall,
    process_chirps_rainfall,
)

# Smallest single-precision floating-point number
MIN_FLOAT = -3.4028234663852886e38


class MockFile(BytesIO):
    """A mock file object that adds a fileno method."""

    def fileno(self):
        return 1


@pytest.fixture
def mock_get_path():
    with patch("dart_pipeline.paths.get_path") as mock_path:
        mock_path.return_value = "/mock/path"
        yield mock_path


@pytest.fixture
def mock_plot_timeseries():
    with patch("dart_pipeline.plots.plot_timeseries") as mock_plot:
        yield mock_plot


@pytest.fixture
def mock_read_excel():
    with patch("pandas.read_excel") as mock_read:
        mock_read.return_value = pd.DataFrame(
            {
                "ano": [2023, 2023],
                "semana": [1, 2],
                "tipo_dx": ["C", "P"],
                "n": [10, 20],
            }
        )
        yield mock_read


@pytest.fixture
def mock_os_walk():
    with patch("os.walk") as mock_walk:
        mock_walk.return_value = [
            (
                "/mock/source/path",
                ["subdir"],
                ["casos_dengue_nacional.xlsx", "casos_dengue_region1.xlsx"],
            )
        ]
        yield mock_walk


@pytest.mark.parametrize(
    "admin_level, expected_admin_level_1, expected_plot_calls, should_raise",
    [
        ("0", "", 1, False),  # Admin level 0
        ("1", "Region1", 1, False),  # Admin level 1
        ("2", None, 0, True),  # Invalid admin level
    ],
)
def test_process_dengueperu(
    admin_level,
    expected_admin_level_1,
    expected_plot_calls,
    should_raise,
    mock_get_path,
    mock_plot_timeseries,
    mock_read_excel,
    mock_os_walk,
):
    if should_raise:
        match = f"Invalid admin level: {admin_level}"
        with pytest.raises(ValueError, match=match):
            process_dengueperu(admin_level=admin_level)
    else:
        master = process_dengueperu(admin_level=admin_level, plots=True)

        # Validate the output DataFrame
        assert isinstance(master, pd.DataFrame)
        assert master["admin_level_0"].iloc[0] == "Peru"
        assert master["admin_level_1"].iloc[0] == expected_admin_level_1
        assert master["metric"].tolist() == [
            "Confirmed Dengue Cases",
            "Probable Dengue Cases",
        ]

        # Check the mock calls
        mock_read_excel.assert_called()


@patch("geopandas.read_file")
@patch("dart_pipeline.process.get_chirps_rainfall_data_path")
@patch("dart_pipeline.process.get_shapefile")
@patch("rasterio.open")
@patch("dart_pipeline.process.get_path")
@patch("matplotlib.pyplot.savefig")
def test_process_gadm_chirps_rainfall(
    mock_savefig,
    mock_get_path,
    mock_raster_open,
    mock_get_shapefile,
    mock_get_chirps_rainfall_data_path,
    mock_read_file,
):
    iso3 = "VNM"
    admin_level = "3"
    partial_date = "2023-05"
    plots = False

    # Mock the file paths
    mock_get_chirps_rainfall_data_path.return_value = "mocked_file.tif"
    mock_get_shapefile.return_value = "mocked_shapefile.shp"
    mock_get_path.return_value = "mocked_output_path"

    # Mock rasterio dataset and set up a test array for rainfall data
    mock_dataset = MagicMock()
    mock_dataset.bounds = MagicMock()
    mock_dataset.bounds.left = -180
    mock_dataset.bounds.bottom = -90
    mock_dataset.bounds.right = 180
    mock_dataset.bounds.top = 90
    mock_raster_open.return_value.__enter__.return_value = mock_dataset

    # Mock GeoDataFrame and region data
    mock_gdf = MagicMock(spec=gpd.GeoDataFrame)
    region_geometry = Polygon([(-180, -90), (0, -90), (0, 0), (-180, 0)])
    mock_region = MagicMock()
    mock_region.geometry = region_geometry
    mock_region.__getitem__.side_effect = lambda key: {
        "COUNTRY": "Vietnam",
        "NAME_1": "Hanoi",
        "NAME_2": "District 1",
        "NAME_3": "Ward 1",
    }[key]
    mock_gdf.iterrows.return_value = [(0, mock_region)]
    mock_gdf.to_crs.return_value = mock_gdf
    mock_read_file.return_value = mock_gdf

    # Call the function
    output = process_gadm_chirps_rainfall(
        iso3, int(admin_level), partial_date, plots=plots
    )

    # Verify the output DataFrame
    expected_data = {
        "admin_level_0": ["Vietnam"],
        "admin_level_1": ["Hanoi"],
        "admin_level_2": ["District 1"],
        "admin_level_3": ["Ward 1"],
        "year": [2023],
        "month": [5],
        "day": [None],
        "rainfall": [0],
    }
    expected_df = pd.DataFrame(expected_data).astype(object)
    pd.testing.assert_frame_equal(output, expected_df)

    # Verify the interaction with GeoDataFrame and rasterio
    assert mock_gdf.to_crs.call_count == 2


@patch("dart_pipeline.process.get_path")
@patch("rasterio.open")
@patch("dart_pipeline.process.get_chirps_rainfall_data_path")
def test_process_chirps_rainfall(mock_data_path, mock_raster_open, mock_get_path):
    partial_date = "2023-05"
    plots = False

    mock_data_path.return_value = "mocked_file.tif"
    mock_get_path.return_value = "mocked_output_path"

    # Mock rasterio dataset and set up a test array for rainfall data
    mock_dataset = MagicMock()
    mock_data = np.array(
        [[0, 1, 2], [3, 4, -3.4028234663852886e38], [np.nan, -9999, 5]]
    )
    mock_dataset.read.return_value = mock_data
    mock_raster_open.return_value.__enter__.return_value = mock_dataset

    output = process_chirps_rainfall(partial_date, plots=plots)

    # Verify the output DataFrame
    expected_data = {"year": [2023], "month": [5], "day": [None], "rainfall": [0.0]}
    expected_df = pd.DataFrame(expected_data).astype(object)
    pd.testing.assert_frame_equal(output, expected_df)
