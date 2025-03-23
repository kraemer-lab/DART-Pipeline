"""Tests for process functions in process.py."""

from io import BytesIO

from unittest.mock import patch, MagicMock

from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from dart_pipeline.metrics.chirps import (
    process_gadm_chirps_rainfall,
    process_chirps_rainfall,
    chirps_rainfall_data,
)
from dart_pipeline.types import URLCollection

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


def test_chirps_rainfall_data():
    base_url = "https://data.chc.ucsb.edu"
    assert chirps_rainfall_data("2020") == [
        URLCollection(
            f"{base_url}/products/CHIRPS-2.0/global_annual/tifs",
            ["chirps-v2.0.2020.tif"],
            relative_path="global_annual",
        )
    ]

    base_url = "https://data.chc.ucsb.edu"
    assert chirps_rainfall_data("2020-01") == [
        URLCollection(
            f"{base_url}/products/CHIRPS-2.0/global_annual/tifs",
            ["chirps-v2.0.2020.tif"],
            relative_path="global_annual",
        ),
        URLCollection(
            f"{base_url}/products/CHIRPS-2.0/global_monthly/tifs",
            ["chirps-v2.0.2020.01.tif.gz"],
            relative_path="global_monthly/2020",
        ),
        URLCollection(
            f"{base_url}/products/CHIRPS-2.0/global_daily/tifs/p05/2020",
            [f"chirps-v2.0.2020.01.{day:02d}.tif.gz" for day in range(1, 32)],
            relative_path="global_daily/2020/01",
        ),
    ]


@patch("geopandas.read_file")
@patch("dart_pipeline.metrics.chirps.get_chirps_rainfall_data_path")
@patch("dart_pipeline.util.get_shapefile")
@patch("rasterio.open")
@patch("dart_pipeline.paths.get_path")
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


@patch("dart_pipeline.paths.get_path")
@patch("rasterio.open")
@patch("dart_pipeline.metrics.chirps.get_chirps_rainfall_data_path")
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
