"""Test the plotting functions."""
from datetime import date
from pathlib import Path
from unittest.mock import patch, MagicMock
import re
import tempfile

from matplotlib import pyplot as plt
from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import pandas as pd

from dart_pipeline.plots import \
    plot_heatmap, \
    plot_gadm_micro_heatmap, \
    plot_gadm_macro_heatmap, \
    plot_timeseries, \
    plot_scatter


@patch('matplotlib.pyplot.savefig')
@patch('matplotlib.pyplot.colorbar')
@patch('matplotlib.pyplot.title')
@patch('matplotlib.pyplot.imshow')
def test_plot_heatmap(
    mock_imshow, mock_title, mock_colorbar, mock_savefig
):
    # Including a 0 value to be converted to NaN
    data = np.array([[1, 2], [0.0, 4]])
    title = 'Test Heatmap'
    colourbar_label = 'Test Colourbar'
    path = Path('test/path/file.png')

    plot_heatmap(data, title, colourbar_label, path)

    # Assert that zeroes are converted to NaNs
    msg = 'Zeros in data should be converted to NaN.'
    assert np.isnan(data[1, 0]), msg

    # Verify plt.imshow called with modified data and colourmap
    mock_imshow.assert_called_once_with(data, cmap='coolwarm', origin='upper')

    # Verify colourbar and title set up correctly
    mock_title.assert_called_once_with(title)
    mock_colorbar.assert_called_once_with(label=colourbar_label)

    # Check the path generation
    expected_path = Path('test/path/file.png')
    mock_savefig.assert_called_once_with(expected_path)

    # Ensure directories are created
    msg = 'The directories should be created.'
    assert expected_path.parent.exists(), msg


@patch('matplotlib.pyplot.colorbar')
@patch('matplotlib.pyplot.savefig')
@patch('matplotlib.pyplot.subplots')
@patch('geopandas.GeoDataFrame.plot')
def test_plot_gadm_micro_heatmap(
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
            plot_gadm_micro_heatmap(
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


@patch('matplotlib.pyplot.subplots')
@patch('matplotlib.pyplot.colorbar')
@patch('matplotlib.pyplot.savefig')
def test_plot_gadm_macro_heatmap(
    mock_savefig, mock_colorbar, mock_subplots, tmp_path
):
    # Sample data and parameters
    data = np.array([[1, 2], [3, 4]])
    origin = 'upper'
    extent = [100, 200, 100, 200]
    limits = [10, 5, 20, 15]
    zorder = 1
    title = 'Test Heatmap'
    colourbar_label = 'Sample Label'
    path = tmp_path / 'plot.png'
    geometry = gpd.points_from_xy([100], [100])
    gdf = gpd.GeoDataFrame({'col': [1]}, geometry=geometry)

    # Mock figure, axes and colour bar
    fig, ax = plt.figure(), plt.axes()
    mock_subplots.return_value = (fig, ax)
    mock_cbar = MagicMock()
    mock_colorbar.return_value = mock_cbar

    # Call the function
    plot_gadm_macro_heatmap(
        data, origin, extent, limits, gdf, zorder, title, colourbar_label, path
    )

    # Check plt.subplots() was called once
    mock_subplots.assert_called_once()

    # Check plt.colorbar() was called once
    mock_colorbar.assert_called_once()
    _, colorbar_kwargs = mock_colorbar.call_args
    assert colorbar_kwargs['ax'] == ax
    assert colorbar_kwargs['label'] == colourbar_label

    # Check set_ticklabels() was not called
    mock_cbar.set_ticklabels.assert_not_called()

    # Check plt.savefig was called with the correct path
    mock_savefig.assert_called_once_with(path)

    # Reset mocks
    mock_subplots.reset_mock()
    mock_colorbar.reset_mock()
    mock_savefig.reset_mock()

    # Mock figure, axes and colour bar
    mock_ax = MagicMock()
    mock_cbar = MagicMock()
    mock_subplots.return_value = (None, mock_ax)
    mock_colorbar.return_value = mock_cbar

    # Define expected tick values after exponential transformation
    expected_ticks = [2, 3]
    expected_tick_labels = [f'{np.exp(tick):.2f}' for tick in expected_ticks]
    # Configure the colour bar to return mock ticks within the data range
    mock_cbar.get_ticks.return_value = [1, 2, 3, 4]

    # Call the function
    plot_gadm_macro_heatmap(
        data, origin, extent, limits, gdf, zorder, title, colourbar_label,
        path, log_plot=True
    )

    # Ensure cbar.set_ticks and cbar.set_ticklabels are called
    mock_cbar.set_ticks.assert_called_once_with(expected_ticks)
    mock_cbar.set_ticklabels.assert_called_once_with(expected_tick_labels)

    # Ensure savefig was called with the correct path
    mock_savefig.assert_called_once_with(path)


@patch('matplotlib.pyplot.close')
@patch('matplotlib.pyplot.savefig')
@patch('pathlib.Path.mkdir')
def test_plot_timeseries(mock_mkdir, mock_savefig, mock_close):
    # Mock data
    data = {
        'date': pd.date_range(start='2020-01-01', periods=10, freq='ME'),
        'value': [10, 15, 20, 25, 30, 35, 40, 45, 50, 55],
        'metric': ['metric1'] * 5 + ['metric2'] * 5,
        'year': [2020, 2021] * 5
    }
    df = pd.DataFrame(data)
    title = 'Test Time Series Plot'
    path = Path('/mock/path/to/timeseries_plot.png')

    # Call the function
    plot_timeseries(df, title, path)

    # Check if directories were created
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    # Check if the plot was saved
    mock_savefig.assert_called_once_with(path)
    # Verify plot components
    ax = plt.gca()
    assert ax.get_title() == title, f'{ax.get_title()} != {title}'
    assert ax.get_xlabel() == 'Year', f'{ax.get_xlabel()} != Year'
    assert ax.get_ylabel() == 'Cases', f'{ax.get_ylabel()} != Cases'
    # Check legend
    legend_texts = [text.get_text() for text in ax.get_legend().get_texts()]
    assert legend_texts == ['metric1', 'metric2'], \
        f'{legend_texts} != ["metric1", "metric2"]'
    # Check x-axis limits
    epoch_offset = date(1970, 1, 1).toordinal() - date(1, 1, 1).toordinal() + 1
    expected_xlim = (
        date(2020, 1, 1).toordinal() - epoch_offset,
        date(2021, 12, 31).toordinal() - epoch_offset,
    )
    actual_xlim = ax.get_xlim()
    assert actual_xlim == expected_xlim, f'{actual_xlim} != {expected_xlim}'
    # Ensure plt.close was called
    plt.close('all')  # Clean up any remaining figures


@patch('matplotlib.pyplot.close')
@patch('matplotlib.pyplot.savefig')
@patch('pathlib.Path.mkdir')
def test_plot_scatter(mock_mkdir, mock_savefig, mock_close):
    # Mock data
    x = np.array([10, 20, 30])
    y = np.array([40, 50, 60])
    z = np.array([1, 2, 3])
    title = 'Test Scatter Plot'
    colourbar_label = 'Test Colourbar Label'
    path = Path('/mock/path/to/scatter_plot.png')

    # Call the function
    plot_scatter(x, y, z, title, colourbar_label, path)

    # Check if directories were created
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    # Check if the plot was saved
    mock_savefig.assert_called_once_with(path)
    # Verify plot components
    assert plt.gca().get_title() == title, 'Title mismatch'
    assert plt.gca().get_xlabel() == 'Longitude', 'X-label mismatch'
    assert plt.gca().get_ylabel() == 'Latitude', 'Y-label mismatch'
    # Check the colorbar label
    colorbar = plt.gcf().get_axes()[-1]  # The last axis is the colorbar
    assert colorbar.get_ylabel() == colourbar_label, 'Colourbar label mismatch'
