"""Test the plotting functions."""
from pathlib import Path
from unittest.mock import patch, MagicMock
import re
import tempfile

from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import pytest

from dart_pipeline.plots import plot_heatmap, plot_gadm_heatmap


@patch('matplotlib.pyplot.savefig')
@patch('matplotlib.pyplot.colorbar')
@patch('matplotlib.pyplot.title')
@patch('matplotlib.pyplot.imshow')
def test_plot_heatmap(
    mock_imshow, mock_title, mock_colorbar, mock_savefig
):
    # Create a temporary directory to use as a mock output path
    with tempfile.TemporaryDirectory() as temp_dir:
        # Mock output_path function to return the temp directory
        def mock_output_path(source):
            return Path(temp_dir) / 'output'

        source = 'example/source'
        # Including a 0 value to be converted to NaN
        data = np.array([[1, 2], [0.0, 4]])
        pdate = '2023-10-15'
        title = 'Test Heatmap'
        colourbar_label = 'Test Colourbar'

        # Patch the output_path function within this test's scope
        with patch(
            'dart_pipeline.plots.output_path', side_effect=mock_output_path
        ):
            plot_heatmap(source, data, pdate, title, colourbar_label)

            # Assert that zeroes are converted to NaNs
            msg = "Zeros in data should be converted to NaN."
            assert np.isnan(data[1, 0]), msg

            # Verify plt.imshow called with modified data and colourmap
            mock_imshow.assert_called_once_with(
                data, cmap='coolwarm', origin='upper'
            )

            # Verify colourbar and title set up correctly
            mock_colorbar.assert_called_once_with(label=colourbar_label)
            mock_title.assert_called_once_with(title)

            # Check the path generation
            sanitized_title = re.sub(r'[<>:"/\\|?*]', '_', pdate)
            expected_path = Path(temp_dir) / 'output/2023/10/15' / \
                (sanitized_title + '.png')
            mock_savefig.assert_called_once_with(expected_path)

            # Ensure directories are created
            msg = 'The directories should be created.'
            assert expected_path.parent.exists(), msg


@patch('matplotlib.pyplot.colorbar')
@patch('matplotlib.pyplot.savefig')
@patch('matplotlib.pyplot.subplots')
@patch('geopandas.GeoDataFrame.plot')
def test_plot_gadm_heatmap(
    mock_gdf_plot, mock_subplots, mock_savefig, mock_colorbar
):
    # Create a temporary directory to use as a mock output path
    with tempfile.TemporaryDirectory() as temp_dir:
        # Mock output_path function to return the temp directory
        def mock_output_path(source):
            return Path(temp_dir) / 'output'

        # Set up mock figure and axis
        mock_fig = MagicMock()
        mock_ax = MagicMock()
        mock_subplots.return_value = (mock_fig, mock_ax)

        # Define input parameters
        source = 'example/source'
        # Including a 0 value to be converted to NaN
        data = np.array([[1, 2], [0.0, 4]])
        pdate = '2023-10-15'
        title = 'Test GADM Heatmap'
        colourbar_label = 'Test Colourbar'
        extent = [100, 105, 20, 25]

        # Create a mock GeoDataFrame and region with geometry bounds
        polygon = Polygon([(100, 20), (105, 20), (105, 25), (100, 25)])
        gdf = gpd.GeoDataFrame(geometry=[polygon])
        region = gpd.GeoDataFrame(geometry=[polygon]).iloc[0]

        # Patch the output_path function within this test's scope
        with patch(
            'dart_pipeline.plots.output_path', side_effect=mock_output_path
        ):
            plot_gadm_heatmap(
                source, data, gdf, pdate, title, colourbar_label, region,
                extent
            )

            # Check that subplots were created
            mock_subplots.assert_called_once()

            # Verify imshow is called with correct data and extent
            mock_ax.imshow.assert_called_once_with(
                data, cmap='coolwarm', origin='upper', extent=extent
            )

            # Check colorbar and labels
            mock_colorbar.assert_called_once_with(
                mock_ax.imshow.return_value, ax=mock_ax, label=colourbar_label
            )
            mock_ax.set_title.assert_called_once_with(title)
            mock_ax.set_xlim.assert_called_once_with(100, 105)
            mock_ax.set_ylim.assert_called_once_with(20, 25)
            mock_ax.set_xlabel.assert_called_once_with('Longitude')
            mock_ax.set_ylabel.assert_called_once_with('Latitude')

            # Check that gdf.plot was called twice
            msg = 'The plot should have been called twice for overlays.'
            assert mock_gdf_plot.call_count == 2, msg

            # Check the path generation and file save
            sanitized_title = re.sub(r'[<>:"/\\|?*]', '_', title)
            expected_path = Path(temp_dir) / 'output/2023/10/15' / \
                (sanitized_title + '.png')
            mock_savefig.assert_called_once_with(expected_path)

            # Ensure directories are created
            msg = 'The directories should be created.'
            assert expected_path.parent.exists(), msg
