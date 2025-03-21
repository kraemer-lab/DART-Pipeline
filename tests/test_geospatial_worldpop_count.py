"""Tests for process functions in geospatial/worldpop_count.py."""

from unittest.mock import patch, MagicMock

from freezegun import freeze_time
import pandas as pd
import pytest
import rasterio

from dart_pipeline.process import process_gadm_worldpopcount


@freeze_time("2025-02-06")
@patch("os.listdir")
@patch("geopandas.gpd.read_file")
@patch("rasterio.open")
def test_process_gadm_worldpopcount(mock_rasterio_open, mock_read_file, mock_listdir):
    # Test case 1: Process valid data
    mock_read_file.return_value = MagicMock()
    mock_rasterio_open.return_value = MagicMock(read=lambda x: [[1, 1], [1, 1]])
    # Run the function with valid data
    output, csv_filename = process_gadm_worldpopcount("VNM", "2020", "2")
    # Assertions for valid data processing
    assert isinstance(output, pd.DataFrame), "Output should be a DataFrame"
    msg = "Expected column missing in output"
    assert "admin_level_0" in output.columns, msg
    assert "metric" in output.columns, "Expected column missing in output"
    msg = "CSV filename does not match expected value"
    expected = "VNM_geospatial_worldpop-count_2020_2025-02-06.csv"
    assert csv_filename == expected, msg

    # Test case 2: Invalid date with day included
    with pytest.raises(ValueError, match="Provide only a year in YYYY format"):
        process_gadm_worldpopcount("VNM", "2020-01-01", admin_level="0")

    # Test case 3: Invalid date with month included
    with pytest.raises(ValueError, match="Provide only a year in YYYY format"):
        process_gadm_worldpopcount("VNM", "2020-01", admin_level="0")

    # Test case 4: Missing raster file, falling back to previous year
    # Simulate missing file for the given year but available fallback file
    mock_listdir.return_value = ["VNM_ppp_v2b_2019_UNadj.tif"]
    mock_rasterio_open.side_effect = [rasterio.errors.RasterioIOError, MagicMock()]
    # Call the function
    output, csv_filename = process_gadm_worldpopcount("VNM", "2020", "0")
    # Check that fallback file was used and output generated
    msg = "Output should be a DataFrame even with fallback file"
    assert isinstance(output, pd.DataFrame), msg
